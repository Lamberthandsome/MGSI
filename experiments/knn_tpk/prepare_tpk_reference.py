"""Compute independent temporal/point/kNN references from native archives.

Runs untimed on the Dask cluster. One task per native file reads only the
needed spatial slices; results combine into tpk_reference.json, the single
validation target for both new-baseline query paths.

Coverage equals the converted stores exactly:
- merra2: available_archive_inventory.json (3329 available days, all hours)
- ecmwf: 10 annual files, all hours (labels derived from first_time cadence)
- gleam: 10 annual files, all days
- ncep: all 40 monthly-product files, all faces ('times' labels)
"""
import argparse
import datetime as dt
import io
import json
import time
from pathlib import Path

import dask
import h5py
import numpy as np
import xarray as xr
from distributed import Client

from tpk_common import (K, MANIFEST_DIR, POINT, WINDOW_1YR, WINDOW_10YR, YANGTZE,
                        box_indices, host, native_fs, nearest_cells, nearest_valid_cells)

ROOT = Path(__file__).resolve().parent


def decode_values(raw, attrs):
    values = np.asarray(raw, dtype=np.float64)
    valid = np.isfinite(values)
    for key in ('_FillValue', 'missing_value'):
        for missing in np.atleast_1d(np.asarray(attrs.get(key, []), dtype=np.float64)).ravel():
            if np.isfinite(missing):
                valid &= values != missing
    values = values * float(np.atleast_1d(np.asarray(attrs.get('scale_factor', 1), dtype=np.float64)).ravel()[0])
    values = values + float(np.atleast_1d(np.asarray(attrs.get('add_offset', 0), dtype=np.float64)).ravel()[0])
    return values, valid


def gleam_valid_mask(inputs):
    """One-day land mask: GLEAM Eb is land-only; ocean cells never hold data."""
    file = inputs['files'][0]
    with native_fs().open_input_file(file['path']) as stream:
        with h5py.File(stream, 'r', rdcc_nbytes=64 * 1024 * 1024) as data:
            field = data[file['variable']]
            first, valid = decode_values(field[0], file.get('attrs', {}))
    return valid


def ncep_dataset(file):
    """Whole-file read plus scipy engine; NCEP archives are classic CDF, not HDF5."""
    fs = native_fs()
    with fs.open_input_file(file['path']) as stream:
        blocks = []
        while True:
            block = stream.read(8 * 1024 * 1024)
            if not block:
                break
            blocks.append(block)
    content = b''.join(blocks)
    assert len(content) == file['bytes']
    return xr.open_dataset(io.BytesIO(content), engine='scipy', decode_times=False, mask_and_scale=False)


def normalized_lon(values):
    return np.where(np.asarray(values, dtype=np.float64) > 180, np.asarray(values, dtype=np.float64) - 360,
                    np.asarray(values, dtype=np.float64))


def ncep_file_task(file, box_slice, cells, windows):
    started = time.perf_counter()
    labels = [str(v) for v in file['times']]
    with ncep_dataset(file) as ds:
        var = ds[file['variable']]
        _, yd, xd = var.dims
        values, valid = decode_values(var.isel({yd: box_slice[0], xd: box_slice[1]}).values, file.get('attrs', {}))
        temporal = {}
        for name, (start, end) in windows.items():
            keep = np.asarray([start <= l[:7] <= end for l in labels])
            if keep.any():
                sub, sub_valid = values[keep], valid[keep]
                temporal[name] = {'sum': float(sub[sub_valid].sum(dtype=np.float64)), 'count': int(sub_valid.sum())}
        cell_stats = []
        for r, c in cells:
            column, column_valid = decode_values(var.isel({yd: r, xd: c}).values, file.get('attrs', {}))
            cell_stats.append({'sum': float(column[column_valid].sum(dtype=np.float64)), 'count': int(column_valid.sum())})
    return {'host': host(), 'path': file['path'], 'temporal': temporal, 'cells': cell_stats,
            'seconds': time.perf_counter() - started}


def merra_file_task(item, attrs, box_slice, cells, windows):
    """One available-day MERRA-2 file: box slice for both windows plus cell columns."""
    started = time.perf_counter()
    fs = native_fs()
    date = item['first_time'][:10]
    with fs.open_input_file(item['path']) as stream:
        with h5py.File(stream, 'r', rdcc_nbytes=32 * 1024 * 1024) as data:
            field = data['PRECTOT']
            values, valid = decode_values(field[:, box_slice[0], box_slice[1]], attrs)
            temporal = {}
            for name, (start, end) in windows.items():
                if start <= date <= end:
                    temporal[name] = {'sum': float(values[valid].sum(dtype=np.float64)), 'count': int(valid.sum())}
            cell_stats = []
            for r, c in cells:
                column, column_valid = decode_values(field[:, r, c], attrs)
                cell_stats.append({'sum': float(column[column_valid].sum(dtype=np.float64)), 'count': int(column_valid.sum())})
    assert fs.get_file_info(item['path']).size == item['bytes']
    return {'host': host(), 'path': item['path'], 'temporal': temporal, 'cells': cell_stats,
            'seconds': time.perf_counter() - started}


def hdf_annual_task(file, dataset, box_slice, cells, windows):
    """One annual/aggregate HDF5 file: full-time box slice plus cell columns."""
    started = time.perf_counter()
    fs = native_fs()
    nt = file['shape'][0]
    if dataset == 'ecmwf':
        first = dt.datetime.fromisoformat(file['first_time'])
        labels = [(first + dt.timedelta(hours=i)).strftime('%Y-%m-%dT%H:%M:%S') for i in range(nt)]
    elif dataset == 'gleam':
        first = dt.datetime.fromisoformat(file['first_time'])
        labels = [(first + dt.timedelta(days=i)).strftime('%Y-%m-%dT%H:%M:%S') for i in range(nt)]
    else:
        labels = [str(v) for v in file['times']]
    with fs.open_input_file(file['path']) as stream:
        with h5py.File(stream, 'r', rdcc_nbytes=32 * 1024 * 1024) as data:
            field = data[file['variable']]
            values, valid = decode_values(field[:, box_slice[0], box_slice[1]], file['attrs'])
            temporal = {}
            for name, (start, end) in windows.items():
                if dataset == 'ncep':
                    keep = np.asarray([start <= l[:7] <= end for l in labels])
                else:
                    keep = np.asarray([start <= l[:10] <= end for l in labels])
                if keep.any():
                    sub, sub_valid = values[keep], valid[keep]
                    temporal[name] = {'sum': float(sub[sub_valid].sum(dtype=np.float64)), 'count': int(sub_valid.sum())}
            cell_stats = []
            for r, c in cells:
                column, column_valid = decode_values(field[:, r, c], file['attrs'])
                cell_stats.append({'sum': float(column[column_valid].sum(dtype=np.float64)), 'count': int(column_valid.sum())})
    return {'host': host(), 'path': file['path'], 'temporal': temporal, 'cells': cell_stats,
            'seconds': time.perf_counter() - started}


def grid_of(dataset, inputs):
    """lat/lon arrays (lon normalized to +/-180) and a representative attrs block."""
    if dataset == 'ncep':
        with ncep_dataset(inputs['files'][0]) as ds:
            lat = np.asarray(ds[[d for d in ds.coords if 'lat' in d.lower()][0]].values, dtype=np.float64)
            lon = normalized_lon(ds[[d for d in ds.coords if 'lon' in d.lower()][0]].values)
        return lat, lon, inputs['files'][0].get('attrs', {})
    entry = inputs['files'][0]
    return np.asarray(entry['lat']), np.asarray(entry['lon']), entry['attrs']


def main(dataset):
    root = MANIFEST_DIR[dataset]
    inputs = json.loads((Path(root) / 'matrix_inputs.json').read_text())[dataset]
    lat, lon, attrs = grid_of(dataset, inputs)
    ri, ci = box_indices(lat, lon, YANGTZE)
    assert len(ri) and len(ci), (dataset, len(ri), len(ci))
    # Land-only variables (GLEAM Eb) have all-fill ocean cells; the fixed point
    # (-174.5, 80.5) falls on open water with no valid candidate nearby. GLEAM
    # uses an exact global nearest-valid search over a one-day mask; the others
    # keep the literal nearest cells (with an all-fill fallback filter).
    if dataset == 'gleam':
        cells = nearest_valid_cells(lat, lon, POINT, K, gleam_valid_mask(inputs))
    else:
        cells = nearest_cells(lat, lon, POINT, 60)
    windows = {'temporal_1yr': WINDOW_1YR[dataset], 'temporal_10yr': WINDOW_10YR[dataset]}

    client = Client('tcp://192.0.2.64:38786')
    client.wait_for_workers(16, timeout=180)
    try:
        if dataset == 'merra2':
            inventory = json.loads((Path(root) / 'available_archive_inventory.json').read_text())
            tasks = [dask.delayed(merra_file_task, pure=False)(item, attrs, (slice(ri[0], ri[-1] + 1), slice(ci[0], ci[-1] + 1)), [(r, c) for r, c, _ in cells], windows) for item in inventory]
        elif dataset == 'ncep':
            tasks = [dask.delayed(ncep_file_task, pure=False)(file, (slice(ri[0], ri[-1] + 1), slice(ci[0], ci[-1] + 1)), [(r, c) for r, c, _ in cells], windows) for file in inputs['files']]
        else:
            tasks = [dask.delayed(hdf_annual_task, pure=False)(file, dataset, (slice(ri[0], ri[-1] + 1), slice(ci[0], ci[-1] + 1)), [(r, c) for r, c, _ in cells], windows) for file in inputs['files']]
        rows = list(dask.compute(*tasks))

        temporal = {name: {'sum': 0.0, 'count': 0} for name in windows}
        cell_totals = [[0.0, 0] for _ in cells]
        for row in rows:
            for name, value in row['temporal'].items():
                temporal[name]['sum'] += value['sum']
                temporal[name]['count'] += value['count']
            for index, stat in enumerate(row['cells']):
                cell_totals[index][0] += stat['sum']
                cell_totals[index][1] += stat['count']
        for value in temporal.values():
            value['mean'] = value['sum'] / value['count'] if value['count'] else None
        for name in windows:
            temporal[name]['window'] = list(windows[name])
        cell_results = []
        for (r, c, distance), (total, count) in zip(cells, cell_totals):
            if count == 0:
                # Ocean/all-fill candidate; skipped by this validity selection.
                continue
            if len(cell_results) >= K:
                break
            cell_results.append({'row': r, 'col': c, 'lat': float(lat[r]), 'lon': float(lon[c]),
                                 'distance_degrees': distance, 'sum': total, 'count': count,
                                 'mean': total / count if count else None})
        assert len(cell_results) == K, (dataset, len(cell_results))
        dlon = float(np.median(np.abs(np.diff(lon))))
        dlat = float(np.median(np.abs(np.diff(lat))))
        reference = {
            'dataset': dataset, 'generated_at': dt.datetime.now().isoformat(),
            'protocol': 'independent native h5py/scipy reads over the exact store coverage; untimed; '
                        'one Dask task per native file; box 102-112E 27-34N; point (-174.5, 80.5); k={} '
                        'nearest cells by Euclidean degree distance among the 60 nearest candidates that '
                        'hold at least one valid observation (land-only GLEAM selects its nearest valid '
                        'cells; global grids keep the literal nearest cells); fill/scale decode identical '
                        'to conversion checks'.format(K),
            'box': YANGTZE, 'point': list(POINT), 'k': K,
            'grid': {'lat_length': len(lat), 'lon_length': len(lon), 'lat_step': dlat, 'lon_step': dlon,
                     'lat_ascending': bool(lat[0] < lat[-1]), 'lon_ascending': bool(lon[0] < lon[-1])},
            'temporal': temporal,
            'point_cell': cell_results[0],
            'knn_cells': cell_results,
            'partial_files': len(rows),
            'worker_seconds_sum': round(sum(r['seconds'] for r in rows), 3),
            'hosts': sorted({r['host'] for r in rows}),
        }
        # Sanity: the 1-year window must be a strict subset of the 10-year window.
        assert temporal['temporal_1yr']['count'] < temporal['temporal_10yr']['count'], temporal
        assert all(c['count'] > 0 for c in cell_results)
        destination = ROOT / (dataset + '_tpk_reference.json')
        destination.write_text(json.dumps(reference, indent=1))
        print('TPK_REFERENCE_READY ' + dataset + ' ' + json.dumps(
            {k: reference['temporal'][k] for k in temporal} | {'point_count': cell_results[0]['count']}))
    finally:
        client.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', choices=['merra2', 'ecmwf', 'ncep', 'gleam'], required=True)
    main(parser.parse_args().dataset)
