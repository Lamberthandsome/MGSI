"""Extract immutable HDF5 physical offsets; queries keep the original NetCDF."""
import hashlib
import json
import math
from pathlib import Path
import sys
import time
import zlib

import h5py
import numpy as np
from hdfs import InsecureClient

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, '/opt/mgsi-experiments/cross_baseline_audit')
from audit_worker import ExactRangeFile


def main():
    started = time.perf_counter()
    plan = json.loads(Path('/opt/mgsi-experiments/alignment_rpc/plan_gleam_1981_1990_spatial.json').read_text())
    source = Path('/opt/mgsi-experiments/alignment_rpc/results_gleam_1981_1990_spatial_rpc.jsonl')
    golden = [json.loads(line) for line in source.read_text().splitlines()]
    (ROOT / 'golden_results.json').write_text(json.dumps(golden))
    tasks = next(case['tasks'] for case in plan['cases'] if case['region'] == 8)
    byfile = {}
    for task in tasks:
        bucket = byfile.setdefault(task['path'], {'time_indices': task['time_indices'], 'time_labels': task['time_labels'], 'entries': {}})
        bucket['entries'].update({entry['id']: entry for entry in task['entries']})
    client = InsecureClient('http://192.0.2.64:50070', user='root')
    result = {'dataset': 'GLEAM', 'version': 'v4.3a', 'units': 'mm.day-1', 'files': [], 'entries': [],
              'boxes': {str(case['region']): case['tasks'][0]['box'] for case in plan['cases']},
              'scope': '1981-1990; every monthly 15th; all eight spatial boxes; native source unchanged.'}
    verification = []
    for fid, (path, bucket) in enumerate(sorted(byfile.items())):
        before = client.status(path)
        reader = ExactRangeFile(path)
        with h5py.File(reader, 'r') as handle:
            variable = handle['Eb']
            cp = variable.id.get_create_plist()
            filters = [cp.get_filter(i)[0] for i in range(cp.get_nfilters())]
            assert filters == [1], filters
            assert variable.dtype.str == '<f4', variable.dtype.str
            chunks = list(variable.chunks)
            shape = list(variable.shape)
            grid = [int(math.ceil(n / float(c))) for n, c in zip(shape, chunks)]
            rows = []
            for tc in sorted({index // chunks[0] for index in bucket['time_indices']}):
                for rc in range(grid[1]):
                    for cc in range(grid[2]):
                        coord = (tc * chunks[0], rc * chunks[1], cc * chunks[2])
                        info = variable.id.get_chunk_info_by_coord(coord)
                        key = (tc * grid[1] + rc) * grid[2] + cc
                        rows.append([key, info.byte_offset or 0, info.size, info.filter_mask])
            entry = {'file_id': fid, 'path': path, 'variable': 'Eb', 'source_bytes': before['length'],
                     'source_mtime': before['modificationTime'], 'shape': shape, 'physical_shape': chunks,
                     'physical_grid': grid, 'physical_chunks': rows, 'filters': filters,
                     'dtype': variable.dtype.str, 'fill': float(variable.attrs['_FillValue'][0]),
                     'scale': float(np.asarray(variable.attrs.get('scale_factor', 1)).flat[0]),
                     'offset': float(np.asarray(variable.attrs.get('add_offset', 0)).flat[0]),
                     'lat': handle['lat'][:].tolist(), 'lon': handle['lon'][:].tolist(),
                     'time_indices': bucket['time_indices'], 'time_labels': bucket['time_labels']}
            assert len(entry['time_indices']) == 12 and len(bucket['entries']) == 500
            for ti in (entry['time_indices'][0], entry['time_indices'][5], entry['time_indices'][-1]):
                for rc, cc in ((0, 0), (grid[1] // 2, grid[2] // 2), (grid[1] - 1, grid[2] - 1)):
                    coord = (ti // chunks[0] * chunks[0], rc * chunks[1], cc * chunks[2])
                    info = variable.id.get_chunk_info_by_coord(coord)
                    assert info.size and info.filter_mask == 0
                    reader.seek(info.byte_offset)
                    raw = zlib.decompress(reader.read(info.size))
                    cube = np.frombuffer(raw, dtype=variable.dtype).reshape(chunks)
                    h = min(chunks[1], shape[1] - coord[1])
                    w = min(chunks[2], shape[2] - coord[2])
                    actual = cube[ti % chunks[0], :h, :w].copy()
                    expected = variable[ti, coord[1]:coord[1]+h, coord[2]:coord[2]+w]
                    assert np.array_equal(actual, expected, equal_nan=True), (path, coord)
                    verification.append({'path': path, 'time_index': ti, 'physical_coordinate': list(coord),
                                         'decoded_sha256': hashlib.sha256(actual.tobytes()).hexdigest()})
            for logical in sorted(bucket['entries'].values(), key=lambda x: (x['rows'], x['cols'])):
                result['entries'].append(dict(logical, file_id=fid, entry_id=len(result['entries'])))
            result['files'].append(entry)
        reader.close()
        after = client.status(path)
        assert (before['length'], before['modificationTime']) == (after['length'], after['modificationTime'])
        print(json.dumps({'physical_index_file_ready': path, 'chunks': len(rows), 'bytes_fetched': reader.bytes_fetched}), flush=True)
    assert len(result['files']) == 10 and len(result['entries']) == 5000
    first = result['files'][0]
    assert all(file['lat'] == first['lat'] and file['lon'] == first['lon'] for file in result['files'])
    result['preparation_seconds'] = time.perf_counter() - started
    output = ROOT / 'physical_index.json'
    output.write_text(json.dumps(result, separators=(',', ':')))
    audit = {'status': 'PASS', 'raw_offset_decode_checks': len(verification), 'checks': verification,
             'physical_index_sha256': hashlib.sha256(output.read_bytes()).hexdigest(),
             'preparation_seconds': result['preparation_seconds'], 'source_files': 10, 'logical_chunks': 5000}
    (ROOT / 'physical_index_validation.json').write_text(json.dumps(audit, indent=2))
    print(json.dumps({key: value for key, value in audit.items() if key != 'checks'}), flush=True)


if __name__ == '__main__':
    main()
