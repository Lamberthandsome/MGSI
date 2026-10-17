"""Build the engine meta + golden reference for a re-chunked NCEP variant (server, PY312).

Usage: python build_meta_ncep.py <variant>

Reads the rewrite manifest and the local rewritten copies, enumerates HDF5
chunk storage per file, emits physical_index_<variant>.json (+ HDFS block map
via the engine's blockmap subcommand), computes yangtze/global/R5 goldens from
the rewritten values, cross-checks R1-R8 against the verified NCEP stage
golden, and writes golden_extra_<variant>.json and slice_info_<variant>.json.
"""
import glob
import json
import math
import os
import subprocess
import sys
from pathlib import Path

import h5py
import numpy as np

ROOT = Path(__file__).resolve().parent
YANGTZE = {'lat_min': 27.0, 'lat_max': 34.0, 'lon_min': 102.0, 'lon_max': 112.0}
CP = ':'.join(['/opt/mgsi-cluster/flink/lib/*', '/app/hadoop-2.7.2/share/hadoop/common/*',
               '/app/hadoop-2.7.2/share/hadoop/common/lib/*', '/app/hadoop-2.7.2/share/hadoop/hdfs/*',
               '/app/hadoop-2.7.2/share/hadoop/hdfs/lib/*'])


def chunk_map(dataset):
    """coordinate tuple -> (offset, size) for every stored chunk (field-name agnostic)."""
    result = {}
    sample = tuple(dataset.id.get_chunk_info(0))
    fields = getattr(dataset.id.get_chunk_info(0), '_fields', None)
    if fields is None:
        raise RuntimeError('unexpected h5py ChunkInfo shape: %r' % (sample,))
    for i in range(dataset.id.get_num_chunks()):
        info = dict(zip(fields, tuple(dataset.id.get_chunk_info(i))))
        coord = tuple(int(x) for x in info['chunk_offset'])
        result[coord] = (int(info['byte_offset']), int(info['size']))
    return result


def aggregate(cells, fill, scale, offset):
    cells = cells.astype(np.float64)
    mask = np.isfinite(cells) & (cells != fill)
    return float((cells[mask]*scale+offset).sum()), int(mask.sum())


def main():
    variant = sys.argv[1]
    original = json.loads((ROOT/'physical_index.with_blocks.json').read_text())
    manifest = json.loads((ROOT/('ncep_'+variant)/'rewrite_manifest.json').read_text())
    files_by_path = {f['path']: f for f in original['files']}
    entries = sorted(original['entries'], key=lambda e: (e['file_id'], e['rows'][0], e['cols'][0]))

    new_files = []
    file_id = 0
    target_to_source = {row['target']: row['source'] for row in manifest}
    for row in sorted(manifest, key=lambda r: r['source']):
        src = files_by_path[row['source']]
        local = ROOT/('ncep_'+variant)/(row['source'].replace('/', '_'))
        with h5py.File(local, 'r') as handle:
            variable = handle[src['variable']]
            chunks = list(variable.chunks)
            grid = [math.ceil(src['shape'][d]/chunks[d]) for d in range(3)]
            stored = chunk_map(variable)
            physical = []
            for tc in range(grid[0]):
                for rc in range(grid[1]):
                    for cc in range(grid[2]):
                        coord = (tc*chunks[0], rc*chunks[1], cc*chunks[2])
                        offset, size = stored[coord]
                        idx = (tc*grid[1]+rc)*grid[2]+cc
                        physical.append([idx, offset, size, 1])
        new_files.append({**{k: v for k, v in src.items() if k not in
                              ('path', 'source_bytes', 'source_mtime', 'physical_chunks', 'physical_shape', 'physical_grid', 'hdfs_blocks', 'dtype')},
                          'file_id': file_id, 'path': row['target'], 'source_bytes': row['target_bytes'],
                          # Sources are big-endian classic NetCDF; the h5py rewrites are little-endian
                          # with identical values, so the variant metadata declares the storage it points to.
                          'dtype': '<f4',
                          # Sources carry no _FillValue; the netCDF-classic default fill applies.
                          # Some stage meta rows were later regenerated with fill=0, which would
                          # drop genuine zero precipitation; align with the verified golden semantics.
                          'fill': 1.0000000200408773e+20,
                          'physical_chunks': physical, 'physical_shape': chunks, 'physical_grid': grid})
        file_id += 1
    path_to_new = {p: i for i, p in enumerate(sorted(files_by_path))}
    old_to_new = {f['file_id']: path_to_new[f['path']] for f in original['files']}
    new_entries = []
    for new_id, e in enumerate(entries):
        new_entries.append({**e, 'file_id': old_to_new[e['file_id']], 'entry_id': new_id})
    meta = {'dataset': original.get('dataset', 'NCEP/NCAR'), 'version': original.get('version'),
            'units': original.get('units'), 'files': new_files, 'entries': new_entries,
            'boxes': original['boxes'],
            'scope': 'NCEP/NCAR re-chunked variant {} of {}'.format(variant, original['scope'])}
    base = ROOT/('physical_index_{}.json'.format(variant))
    base.write_text(json.dumps(meta))
    with_blocks = ROOT/('physical_index_{}_with_blocks.json'.format(variant))
    os.environ['CLASSPATH'] = str(ROOT/'resident-mgsi.jar')+':'+CP
    subprocess.run(['/opt/mgsi-modern/jdk17/bin/java', 'ResidentMGSI', 'blockmap', str(base), str(with_blocks)], check=True)

    golden_r = {row['case']: row['monthly_values'] for row in json.loads((ROOT/'golden_results.json').read_text())}
    prefix = 'ncep_{}'.format(variant)
    lat = np.asarray(new_files[0]['lat']); lon = np.asarray(new_files[0]['lon'])
    boxes = {prefix+'_temporal_yangtze': YANGTZE,
             prefix+'_temporal_global': {'lat_min': float(lat.min()), 'lat_max': float(lat.max()),
                                          'lon_min': float(lon.min()), 'lon_max': float(lon.max())},
             prefix+'_spatial_R5': original['boxes']['5']}
    check = {'ncep_{}_spatial_R{}'.format(variant, r): original['boxes'][str(r)] for r in range(1, 9)}
    all_boxes = {**boxes, **check}
    monthly = {case: {} for case in all_boxes}
    for f in new_files:
        local = ROOT/('ncep_'+variant)/(target_to_source[f['path']].replace('/', '_'))
        with h5py.File(local, 'r') as handle:
            arr = np.asarray(handle[f['variable']][:, :, :])
        for case, box in all_boxes.items():
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
    for r in range(1, 9):
        case = 'ncep_{}_spatial_R{}'.format(variant, r)
        expected = golden_r['ncep_1981_1990_spatial_R{}'.format(r)]
        assert monthly[case].keys() == expected.keys(), case
        for month in expected:
            assert monthly[case][month]['count'] == expected[month]['count'], (case, month, 'count')
            assert math.isclose(monthly[case][month]['sum'], expected[month]['sum'], rel_tol=1e-9, abs_tol=1e-6), (case, month, 'sum')
    rows = [{'case': case, 'monthly_values': months} for case, months in monthly.items() if case in boxes]
    (ROOT/('golden_extra_{}.json'.format(variant))).write_text(json.dumps(rows))
    (ROOT/('slice_info_{}.json'.format(variant))).write_text(json.dumps(
        {'golden_prefix': prefix, 'tile_rows': 47, 'tile_cols': 48, 'variant': variant,
         'files': len(new_files), 'entries': len(new_entries),
         'physical_shape': new_files[0]['physical_shape'], 'physical_grid': new_files[0]['physical_grid']}))
    print('META_READY '+json.dumps({'variant': variant, 'files': len(new_files), 'entries': len(new_entries)}), flush=True)


if __name__ == '__main__':
    main()
