"""Slice the GLEAM physical index to the first K annual files and build the
matching golden reference for the three scalability cases (server, PY312).

Usage: python slice_meta_volume.py K

- Remaps file ids to 0..K-1 and entry ids to 0..M-1 consistently.
- Computes yangtze/global/R5 monthly aggregates from the K files directly.
- Cross-checks every month against the corresponding month of the verified
  full-archive golden (aggregation is per-month independent), then writes
  physical_index_vol{K}yr.json, golden_extra_vol{K}yr.json, slice_info_vol{K}yr.json.
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
K = int(sys.argv[1])
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
    files = sorted(meta['files'], key=lambda f: f['path'])[:K]
    keep = {f['file_id'] for f in files}
    entries = [e for e in meta['entries'] if e['file_id'] in keep]
    remap_file = {}
    for new_id, f in enumerate(files):
        remap_file[f['file_id']] = new_id
        f['file_id'] = new_id
    for new_id, e in enumerate(entries):
        e['file_id'] = remap_file[e['file_id']]
        e['entry_id'] = new_id
    meta['files'] = files
    meta['entries'] = entries
    meta['scope'] = 'volume slice: first {} annual files of {}'.format(K, meta['scope'])
    meta_path = ROOT/('physical_index_vol{}yr.json'.format(K))
    meta_path.write_text(json.dumps(meta))

    prefix = 'gleam_vol{}yr'.format(K)
    lats = files[0]['lat']; lons = files[0]['lon']
    lat = np.asarray(lats); lon = np.asarray(lons)
    boxes = {prefix+'_temporal_yangtze': YANGTZE,
             prefix+'_temporal_global': {'lat_min': min(lats), 'lat_max': max(lats), 'lon_min': min(lons), 'lon_max': max(lons)},
             prefix+'_spatial_R5': meta['boxes']['5']}
    full = {row['case']: row['monthly_values'] for row in json.loads((ROOT/'golden_results.json').read_text())}
    full.update({row['case']: row['monthly_values'] for row in json.loads((ROOT/'golden_extra.json').read_text())})
    full_case = {'gleam_vol{}yr_temporal_yangtze'.format(K): 'gleam_1981_1990_temporal_yangtze',
                 'gleam_vol{}yr_temporal_global'.format(K): 'gleam_1981_1990_temporal_global',
                 'gleam_vol{}yr_spatial_R5'.format(K): 'gleam_1981_1990_spatial_R5'}

    fs = native_fs()
    monthly = {case: {} for case in boxes}
    for f in files:
        variable_name = f.get('variable', 'Eb')
        with fs.open_input_file(f['path']) as stream, h5py.File(stream, 'r') as handle:
            variable = handle[variable_name]
            arr = np.asarray(variable[:, :, :])
            for case, box in boxes.items():
                rsel = np.where((lat >= box['lat_min']) & (lat <= box['lat_max']))[0]
                csel = np.where((lon >= box['lon_min']) & (lon <= box['lon_max']))[0]
                r0, r1, c0, c1 = rsel.min(), rsel.max()+1, csel.min(), csel.max()+1
                rkeep = (lat[r0:r1] >= box['lat_min']) & (lat[r0:r1] <= box['lat_max'])
                ckeep = (lon[c0:c1] >= box['lon_min']) & (lon[c0:c1] <= box['lon_max'])
                window = arr[:, r0:r1, c0:c1]
                for mi, ti in enumerate(f['time_indices']):
                    total, count = aggregate(window[ti][np.ix_(rkeep, ckeep)], f['fill'], f['scale'], f['offset'])
                    month = f['time_labels'][mi][:7]
                    bucket = monthly[case].setdefault(month, {'sum': 0.0, 'count': 0})
                    bucket['sum'] += total
                    bucket['count'] += count

    for case, ref in full_case.items():
        expected = full[ref]
        for month, value in monthly[case].items():
            assert month in expected, (case, month, 'month absent from full golden')
            assert value['count'] == expected[month]['count'], (case, month, 'count')
            assert math.isclose(value['sum'], expected[month]['sum'], rel_tol=1e-9, abs_tol=1e-6), (case, month, 'sum')
    rows = [{'case': case, 'monthly_values': months} for case, months in monthly.items()]
    (ROOT/('golden_extra_vol{}yr.json'.format(K))).write_text(json.dumps(rows))
    (ROOT/('slice_info_vol{}yr.json'.format(K))).write_text(json.dumps(
        {'golden_prefix': prefix, 'files': K, 'entries': len(entries), 'months_per_case': len(rows[0]['monthly_values'])}))
    print('SLICE_READY '+json.dumps({'k': K, 'entries': len(entries),
        'months': len(rows[0]['monthly_values'])}), flush=True)


if __name__ == '__main__':
    main()
