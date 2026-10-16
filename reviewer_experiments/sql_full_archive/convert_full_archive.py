"""Full-archive SQL point-table conversion: size and cost (server, PY312).

Usage: python convert_full_archive.py ncep|merra2|gleam_scope

Materializes the row-per-cell Parquet point table with PyArrow (the same
toolchain convention as the matched-baseline representation preparation):
columns date:string, lat:float32, lon:float32, value:float32 (packed source
float32, fill values included as stored). One Parquet file per source file,
one row group per time plane; written locally then uploaded to HDFS.

Scope per dataset (disclosed in the report):
  ncep        full archive time axis (all 480 native planes)
  merra2      full archive time axis (all 24 hourly planes x 120 files)
  gleam_scope the queried-date scope only (monthly 15th, 120 planes);
             the full daily axis is 23.7B rows and is NOT converted (disk);
             its row count is a stated fact, not an extrapolated cost.

Records per file: rows, parquet bytes (local and HDFS), wall seconds split
into read/explode/write/upload; totals in conversion_report.json.
"""
import glob
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import h5py
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from netCDF4 import Dataset

ROOT = Path(__file__).resolve().parent
SCHEMA = pa.schema([('date', pa.string()), ('lat', pa.float32()), ('lon', pa.float32()), ('value', pa.float32())])


def native_fs():
    parts = os.environ.get('HADOOP_CLASSPATH', os.environ.get('CLASSPATH', '')).split(':')
    os.environ['CLASSPATH'] = ':'.join(v for part in parts for v in (glob.glob(part) if '*' in part else [part]))
    return pafs.HadoopFileSystem('192.0.2.64', 9000, user='root', replication=2, extra_conf={'dfs.replication': '2'})


def read_classic(fs, path, variable):
    """NetCDF-3 classic source (NCEP/NCAR): read via a local temp copy."""
    with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
        tmp_path = tmp.name
    with fs.open_input_file(path) as stream, open(tmp_path, 'wb') as local:
        while True:
            block = stream.read(32*1024*1024)
            if not block:
                break
            local.write(block)
    try:
        with Dataset(tmp_path, 'r', maskandscale=False) as ds:
            return np.asarray(ds.variables[variable][:, :, :])
    finally:
        os.unlink(tmp_path)


def main():
    dataset = sys.argv[1]
    meta_path = ROOT/(dataset+'_meta.json')
    meta = json.loads(meta_path.read_text())
    fs = native_fs()
    out_hdfs = '/user/mgsi-experiments/20261016-sql-fullarchive/'+dataset
    out_local = ROOT/('pointtable_'+dataset)
    out_local.mkdir(exist_ok=True)
    lat = np.asarray(meta['files'][0]['lat'], dtype=np.float32)
    lon = np.asarray(meta['files'][0]['lon'], dtype=np.float32)
    rows_total, bytes_total = 0, 0
    per_file = []
    started = time.perf_counter()
    for f in sorted(meta['files'], key=lambda x: x['path']):
        t0 = time.perf_counter()
        if dataset == 'ncep':
            planes = read_classic(fs, f['path'], f['variable'])
            plane_count = planes.shape[0]
        else:
            with fs.open_input_file(f['path']) as stream, h5py.File(stream, 'r') as handle:
                variable = handle[f['variable']]
                planes = np.asarray(variable[:, :, :])
                plane_count = variable.shape[0]
        times = list(range(plane_count)) if dataset != 'gleam_scope' else list(f['time_indices'])
        label_of_time = {ti: f['time_labels'][mi][:10] for mi, ti in enumerate(f['time_indices'])} \
            if dataset == 'gleam_scope' else {ti: f['time_labels'][ti][:10] for ti in range(len(f['time_labels']))}
        t_read = time.perf_counter()-t0
        name = Path(f['path']).stem+'.parquet'
        target = out_local/name
        t1 = time.perf_counter()
        rows_file = 0
        with pq.ParquetWriter(target, SCHEMA) as writer:
            for ti in times:
                date = label_of_time[ti]
                plane = planes[ti]
                table = pa.table({
                    'date': pa.array(np.full(plane.size, date, dtype=object), pa.string()),
                    'lat': pa.array(np.repeat(lat, lon.size)),
                    'lon': pa.array(np.tile(lon, lat.size)),
                    'value': pa.array(plane.reshape(-1))})
                writer.write_table(table)
                rows_file += plane.size
        t_write = time.perf_counter()-t1
        t2 = time.perf_counter()
        with target.open('rb') as local, fs.open_output_stream(out_hdfs+'/'+name) as remote:
            while True:
                block = local.read(32*1024*1024)
                if not block:
                    break
                remote.write(block)
        t_upload = time.perf_counter()-t2
        stored = fs.get_file_info(out_hdfs+'/'+name)
        per_file.append({'source': f['path'], 'parquet': out_hdfs+'/'+name, 'planes': len(times),
                         'rows': rows_file, 'local_bytes': target.stat().st_size, 'hdfs_bytes': stored.size,
                         'read_seconds': t_read, 'write_seconds': t_write, 'upload_seconds': t_upload})
        rows_total += rows_file
        bytes_total += stored.size
        print('CONVERTED {} rows={:.1f}M bytes={:.1f}MB total={:.1f}s'.format(
            name, rows_file/1e6, stored.size/1048576, t_read+t_write+t_upload), flush=True)
    elapsed = time.perf_counter()-started
    report = {'dataset': dataset,
              'scope': 'full archive time axis' if dataset != 'gleam_scope' else 'queried-date scope (monthly 15th, 120 planes)',
              'schema': {'date': 'string', 'lat': 'float32', 'lon': 'float32', 'value': 'float32 (packed source, fill included)'},
              'files': len(per_file), 'rows_total': rows_total, 'hdfs_bytes_total': bytes_total,
              'bytes_per_row': bytes_total/rows_total,
              'wall_seconds': elapsed, 'rows_per_second': rows_total/elapsed,
              'per_file': per_file,
              'not_converted': {'gleam_full_daily_axis_rows': 3650*1800*3600} if dataset == 'gleam_scope' else None}
    (ROOT/('conversion_report_{}.json'.format(dataset))).write_text(json.dumps(report, indent=2))
    print('CONVERSION_DONE '+json.dumps({k: v for k, v in report.items() if k != 'per_file'}), flush=True)


if __name__ == '__main__':
    main()
