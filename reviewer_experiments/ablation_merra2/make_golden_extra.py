"""Dataset-parameterized golden reference builder (server, PY312).

Usage: python make_golden_extra.py gleam|merra2

Reads the source NetCDF files directly through pyarrow HDFS and computes the
monthly aggregates for the two extra ablation cases:
  temporal_yangtze  — the paper's temporal-query region 102-112E, 27-34N
  temporal_global   — the full native extent
Value rule matches the engine: finite and != fill -> value*scale+offset.
As a self-check it also recomputes R1-R8 and asserts them against the stage's
verified golden_results.json (count equal, sum rel_tol 1e-9) before writing
golden_extra.json. Reads each (file, box) window once for all times.
"""
import glob
import json
import math
import os
import sys
from pathlib import Path

import h5py
import numpy as np
import pyarrow.fs as pafs

ROOT = Path(__file__).resolve().parent
DATASET = sys.argv[1] if len(sys.argv) > 1 else 'gleam'

YANGTZE = {'lat_min': 27.0, 'lat_max': 34.0, 'lon_min': 102.0, 'lon_max': 112.0}


def native_fs():
    parts = os.environ.get('HADOOP_CLASSPATH', os.environ.get('CLASSPATH', '')).split(':')
    os.environ['CLASSPATH'] = ':'.join(v for part in parts for v in (glob.glob(part) if '*' in part else [part]))
    return pafs.HadoopFileSystem('192.0.2.64', 9000, user='root', replication=2, extra_conf={'dfs.replication': '2'})


def aggregate(cells, fill, scale, offset):
    cells = cells.astype(np.float64)
    mask = np.isfinite(cells) & (cells != fill)
    return float((cells[mask]*scale+offset).sum()), int(mask.sum())


def main():
    meta = json.loads((ROOT/'physical_index.with_blocks.json').read_text())
    golden_r = {row['case']: row['monthly_values'] for row in json.loads((ROOT/'golden_results.json').read_text())}
    prefix_r = {c for c in golden_r}
    prefix = next(iter(prefix_r)).rsplit('_spatial_', 1)[0]
    lats = meta['files'][0]['lat']; lons = meta['files'][0]['lon']
    for f in meta['files']:
        assert f['lat'] == lats and f['lon'] == lons

    compute = {prefix+'_temporal_yangtze': YANGTZE,
               prefix+'_temporal_global': {'lat_min': min(lats), 'lat_max': max(lats),
                                            'lon_min': min(lons), 'lon_max': max(lons)}}
    check = {prefix+'_spatial_R'+str(r): meta['boxes'][str(r)] for r in range(1, 9)}
    boxes = {**compute, **check}
    lat = np.asarray(lats); lon = np.asarray(lons)

    fs = native_fs()
    monthly = {case: {} for case in boxes}
    for f in meta['files']:
        variable_name = f.get('variable', {'gleam': 'Eb', 'merra2': 'PRECTOT'}[DATASET])
        before = fs.get_file_info(f['path'])
        assert before.size == f['source_bytes'], (f['path'], before.size, f['source_bytes'])
        with fs.open_input_file(f['path']) as stream, h5py.File(stream, 'r') as handle:
            variable = handle[variable_name]
            assert list(variable.shape) == f['shape']
            for case, box in boxes.items():
                rsel = np.where((lat >= box['lat_min']) & (lat <= box['lat_max']))[0]
                csel = np.where((lon >= box['lon_min']) & (lon <= box['lon_max']))[0]
                r0, r1, c0, c1 = rsel.min(), rsel.max()+1, csel.min(), csel.max()+1
                rkeep = (lat[r0:r1] >= box['lat_min']) & (lat[r0:r1] <= box['lat_max'])
                ckeep = (lon[c0:c1] >= box['lon_min']) & (lon[c0:c1] <= box['lon_max'])
                window = variable[:, r0:r1, c0:c1]
                for mi, ti in enumerate(f['time_indices']):
                    cells = window[ti][np.ix_(rkeep, ckeep)]
                    total, count = aggregate(cells, f['fill'], f['scale'], f['offset'])
                    month = f['time_labels'][mi][:7]
                    bucket = monthly[case].setdefault(month, {'sum': 0.0, 'count': 0})
                    bucket['sum'] += total
                    bucket['count'] += count
        after = fs.get_file_info(f['path'])
        assert (before.size, before.mtime_ns) == (after.size, after.mtime_ns)

    for case, expected in golden_r.items():
        assert monthly[case].keys() == expected.keys(), case
        for month in expected:
            assert monthly[case][month]['count'] == expected[month]['count'], (case, month, 'count')
            assert math.isclose(monthly[case][month]['sum'], expected[month]['sum'], rel_tol=1e-9, abs_tol=1e-6), (case, month, 'sum')
    rows = [{'case': case, 'monthly_values': months} for case, months in monthly.items() if case in compute]
    (ROOT/'golden_extra.json').write_text(json.dumps(rows))
    print('GOLDEN_EXTRA_READY '+json.dumps({'dataset': DATASET, 'prefix': prefix,
        'cases': [r['case'] for r in rows],
        'months': [len(r['monthly_values']) for r in rows],
        'yangtze_total_count': sum(v['count'] for v in rows[0]['monthly_values'].values())}), flush=True)


if __name__ == '__main__':
    main()
