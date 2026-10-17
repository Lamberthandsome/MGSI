"""Temporal/point/kNN queries over the converted Zarr stores via xarray/Dask.

Protocol: one unrecorded warmup followed by five measured runs; the mean and
sample standard deviation of the five are reported. Every run must validate
against the independent
tpk_reference.json before any PASS is recorded. Timing covers store open,
graph construction, compute and final aggregation; initialization separate.
"""
import argparse
import gzip
import hashlib
import json
import time
from pathlib import Path

import dask
import numpy as np
import xarray as xr
from distributed import Client, get_task_stream

from tpk_common import (K, MANIFEST_DIR, WINDOW_1YR, WINDOW_10YR, YANGTZE,
                        box_indices, close_enough, decoded, labels_for, mapper,
                        native_fs, window_mask)

ROOT = Path(__file__).resolve().parent


def json_default(value):
    if isinstance(value, bytes):
        return {'bytes_hex': value.hex()}
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(type(value).__name__)


def validate(result, reference, mode):
    if mode in ('temporal_1yr', 'temporal_10yr'):
        expected = reference['temporal'][mode]
        assert close_enough(result, expected), (mode, result, expected)
        return
    if mode == 'point':
        expected = reference['point_cell']
        assert result['count'] == expected['count'], (result, expected)
        assert abs(result['mean'] - expected['mean']) <= max(1e-9, 1e-9 * abs(expected['mean'])), (result, expected)
        assert result['lat'] == expected['lat'] and result['lon'] == expected['lon'], (result, expected)
        return
    assert mode == 'knn'
    assert len(result) == K
    for observed, expected in zip(result, reference['knn_cells']):
        assert observed['count'] == expected['count'], (observed, expected)
        assert abs(observed['mean'] - expected['mean']) <= max(1e-9, 1e-9 * abs(expected['mean'])), (observed, expected)
        assert observed['lat'] == expected['lat'] and observed['lon'] == expected['lon'], (observed, expected)


def zarr_temporal(dataset, preparation, mode):
    window = (WINDOW_1YR if mode == 'temporal_1yr' else WINDOW_10YR)[dataset]
    graphs, opened, matched = [], [], 0
    try:
        for path in preparation['zarr']['stores']:
            ds = xr.open_zarr(mapper(path), consolidated=True, decode_times=False, mask_and_scale=False)
            opened.append(ds)
            labels = labels_for(dataset, ds)
            indices = np.flatnonzero(window_mask(dataset, labels, window))
            matched += len(indices)
            if not len(indices):
                # Yearly sub-stores outside the requested window contribute nothing.
                continue
            ri, ci = box_indices(ds.lat.values, ds.lon.values, YANGTZE)
            value = decoded(ds.value.isel(time=indices, lat=ri, lon=ci))
            graphs.extend([value.sum(skipna=True, dtype=np.float64), value.count()])
        assert matched, (dataset, mode)
        results = dask.compute(*graphs)
        total = float(sum(float(v) for v in results[0::2]))
        count = int(sum(int(v) for v in results[1::2]))
        return {'sum': total, 'count': count, 'mean': total / count if count else None}
    finally:
        for ds in opened:
            ds.close()


def zarr_cells(dataset, preparation, reference, cell_count):
    """Time-mean and valid count for the first cell_count reference cells."""
    cells = reference['knn_cells'][:cell_count]
    graphs, opened = [], []
    try:
        for path in preparation['zarr']['stores']:
            ds = xr.open_zarr(mapper(path), consolidated=True, decode_times=False, mask_and_scale=False)
            opened.append(ds)
            lat, lon = ds.lat.values, ds.lon.values
            rows = np.asarray([cell['row'] for cell in cells])
            cols = np.asarray([cell['col'] for cell in cells])
            for cell in cells:
                assert lat[cell['row']] == cell['lat'] and lon[cell['col']] == cell['lon'], (path, cell)
            value = decoded(ds.value.isel(lat=xr.DataArray(rows, dims='cell'), lon=xr.DataArray(cols, dims='cell')))
            graphs.extend([value.sum(dim='time', skipna=True, dtype=np.float64), value.count(dim='time')])
        results = dask.compute(*graphs)
        out = []
        for index, cell in enumerate(cells):
            total = sum(float(v[index]) for v in results[0::2])
            number = sum(int(v[index]) for v in results[1::2])
            out.append({'lat': cell['lat'], 'lon': cell['lon'], 'sum': total, 'count': number,
                        'mean': total / number if number else None})
        return out
    finally:
        for ds in opened:
            ds.close()


def run_once(dataset, mode, preparation, reference):
    if mode in ('temporal_1yr', 'temporal_10yr'):
        return zarr_temporal(dataset, preparation, mode)
    if mode == 'point':
        return zarr_cells(dataset, preparation, reference, 1)[0]
    return zarr_cells(dataset, preparation, reference, K)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', choices=['merra2', 'ecmwf', 'ncep', 'gleam'], required=True)
    parser.add_argument('--mode', choices=['temporal_1yr', 'temporal_10yr', 'point', 'knn'], required=True)
    args = parser.parse_args()
    dataset, mode = args.dataset, args.mode
    name = 'zarr_' + mode + '_' + dataset
    output = ROOT / (name + '_results.jsonl')
    assert not output.exists(), 'Fresh directory for retries; preserve passed records: ' + str(output)

    root = MANIFEST_DIR[dataset]
    inputs = json.loads((Path(root) / 'matrix_inputs.json').read_text())[dataset]
    preparation = json.loads((Path(root) / (dataset + '_preparation.json')).read_text())
    reference = json.loads((ROOT / (dataset + '_tpk_reference.json')).read_text())
    probe = native_fs().get_file_info(preparation['zarr']['stores'][0])
    assert probe.type.name == 'Directory', probe

    started = time.perf_counter()
    client = Client('tcp://192.0.2.64:38786')
    client.wait_for_workers(16, timeout=180)
    workers = client.scheduler_info(n_workers=-1)['workers']
    assert len(workers) == 16 and sum(v['nthreads'] for v in workers.values()) == 48
    hosts = {v['host'] for v in workers.values()}
    initialization = time.perf_counter() - started
    (ROOT / (name + '_initialization.json')).write_text(json.dumps(
        {'workers': {a: {k: v.get(k) for k in ('host', 'nthreads', 'memory_limit', 'status')} for a, v in workers.items()},
         'seconds_separate': initialization}, default=json_default, indent=1))
    names = ['tpk_common.py', 'run_tpk_dask.py', dataset + '_tpk_reference.json']
    (ROOT / (name + '_pin.json')).write_text(json.dumps(
        {n: hashlib.sha256((ROOT / n).read_bytes()).hexdigest() for n in names}, indent=1))
    try:
        runs = []
        for repeat in range(6):
            label = 'warmup' if repeat == 0 else 'measured'
            with get_task_stream(client=client) as trace:
                timer = time.perf_counter()
                result = run_once(dataset, mode, preparation, reference)
                elapsed = time.perf_counter() - timer
            validate(result, reference, mode)
            active = {workers[t['worker']]['host'] for t in trace.data if t['worker'] in workers}
            # Small temporal/point/kNN graphs may not generate enough tasks to reach
            # every host; actual participation is recorded per run instead of asserted.
            assert active, (label, 'no data hosts observed')
            trace_path = ROOT / (name + '_run{}_tasks.json.gz'.format(repeat))
            with gzip.open(trace_path, 'wt') as stream:
                json.dump(trace.data, stream, default=json_default)
            runs.append({'label': label, 'seconds': elapsed, 'result': result,
                         'task_events': len(trace.data), 'active_data_hosts': sorted(active),
                         'task_trace_file': trace_path.name,
                         'task_trace_sha256': hashlib.sha256(trace_path.read_bytes()).hexdigest()})
            print('TPK_ZARR_RUN ' + json.dumps({'dataset': dataset, 'mode': mode, 'run': label,
                                                'seconds': elapsed}), flush=True)
        measured = [r['seconds'] for r in runs[1:]]
        assert len(measured) == 5
        statistic = float(np.mean(measured))
        sample_std = float(np.std(measured, ddof=1))
        record = {'status': 'PASS', 'method': 'xarray_zarr', 'dataset': dataset, 'mode': mode,
                  'protocol': 'one unrecorded warmup; five measured runs; mean and sample standard '
                              'deviation of the five reported; no persist/result cache; '
                              'initialization separate; OS/HDFS caches uncontrolled and disclosed',
                  'measured_runs': measured,
                  'reported_statistic': statistic, 'sample_std': sample_std,
                  'statistic_rule': 'mean±sample_std',
                  'result': runs[-1]['result'], 'runs': runs, 'initialization_seconds_separate': initialization,
                  'scope': inputs['scope'], 'store_paths': preparation['zarr']['stores']}
        with output.open('a') as stream:
            stream.write(json.dumps(record, allow_nan=False, default=json_default) + '\n')
        (ROOT / (name + '_state.json')).write_text(json.dumps({'status': 'COMPLETE', 'mode': mode, 'dataset': dataset}))
        print('TPK_ZARR_QUERY_PASS ' + json.dumps({'dataset': dataset, 'mode': mode,
                                                   'measured': measured, 'statistic': statistic}), flush=True)
    finally:
        client.close()


if __name__ == '__main__':
    main()
