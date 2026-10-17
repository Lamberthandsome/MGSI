"""Shared definitions for the temporal/point/kNN baseline batch.

Query definitions follow the original manuscript protocol:
- temporal: Yangtze box 102-112E, 27-34N, all target timesteps of a 1-year and a
  10-year window; result is the window sum/count/mean over valid values.
- point: fixed target point (-174.5, 80.5), nearest grid cell (Euclidean in
  degree space, matching the original KD-tree), value = time-mean over the full
  archive plus valid count.
- knn: the k=10 nearest cells to the same point, each with time-mean and count.
"""
import glob
import json
import os
import socket

import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs

YANGTZE = {'lon_min': 102.0, 'lon_max': 112.0, 'lat_min': 27.0, 'lat_max': 34.0}
POINT = (-174.5, 80.5)
K = 10

# 1-year window per dataset: first calendar year; 10-year window: full span.
WINDOW_1YR = {'merra2': ('1980-01-01', '1980-12-31'), 'ecmwf': ('1980-01-01', '1980-12-31'),
              'ncep': ('1981-01', '1981-12'), 'gleam': ('1981-01-01', '1981-12-31'),
              'chirps': ('1981-01-01', '1981-12-31')}
WINDOW_10YR = {'merra2': ('1980-01-01', '1989-12-31'), 'ecmwf': ('1980-01-01', '1989-12-31'),
               'ncep': ('1981-01', '1990-12'), 'gleam': ('1981-01-01', '1990-12-31'),
               'chirps': ('1981-01-01', '1990-12-31')}

WORKERS = ['192.0.2.65', '192.0.2.66', '192.0.2.71', '192.0.2.72']
HOSTS = set(WORKERS)

# Server-side manifest roots for each dataset (matrix_inputs.json + preparation).
MANIFEST_DIR = {
    'merra2': '/opt/mgsi-experiments/20261011-matched-baselines-95pct/merra2_spatial',
    'ecmwf': '/opt/mgsi-experiments/20261011-matched-baselines-95pct/ecmwf_spatial',
    'ncep': '/opt/mgsi-experiments/20261011-matched-baselines-95pct/ncep_spatial',
    'gleam': '/opt/mgsi-experiments/20261011-matched-baselines-90pct/gleam_mgsi',
}

_FS = None


def _hadoop_env():
    """Make libhdfs usable regardless of how this process was started."""
    home = '/app/hadoop-2.7.2'
    os.environ.setdefault('HADOOP_HOME', home)
    os.environ.setdefault('ARROW_LIBHDFS_DIR', home + '/lib/native')
    os.environ.setdefault('JAVA_HOME', '/usr/lib/jvm/java-1.8.0-openjdk')
    if not os.environ.get('CLASSPATH'):
        import subprocess
        source = os.environ.get('HADOOP_CLASSPATH')
        if not source:
            source = subprocess.run([home + '/bin/hadoop', '--config', '/opt/mgsi-cluster/hadoop-conf', 'classpath'],
                                    capture_output=True, text=True, timeout=60).stdout.strip()
        parts = source.split(':')
        os.environ['CLASSPATH'] = ':'.join(v for part in parts for v in (glob.glob(part) if '*' in part else [part]))


def native_fs():
    global _FS
    if _FS is None:
        _hadoop_env()
        parts = os.environ.get('HADOOP_CLASSPATH', os.environ.get('CLASSPATH', '')).split(':')
        os.environ['CLASSPATH'] = ':'.join(v for part in parts for v in (glob.glob(part) if '*' in part else [part]))
        _FS = pafs.HadoopFileSystem('192.0.2.64', 9000, user='root', replication=2, extra_conf={'dfs.replication': '2'})
    return _FS


def mapper(path):
    from fsspec.implementations.arrow import ArrowFSWrapper
    from fsspec.mapping import FSMap
    return FSMap(path, ArrowFSWrapper(native_fs()), check=False)


def host():
    return socket.gethostbyname(socket.gethostname())


def labels_for(dataset, ds):
    """Time labels as strings for one opened zarr store."""
    values = ds.month.values if dataset == 'ncep' else ds.time.values
    return np.asarray([str(v) for v in values])


def window_mask(dataset, labels, window):
    start, end = window
    if dataset == 'ncep':
        return np.asarray([start <= l[:7] <= end for l in labels])
    return np.asarray([start <= l[:10] <= end for l in labels])


def box_indices(lat, lon, box):
    ri = np.flatnonzero((lat >= box['lat_min']) & (lat <= box['lat_max']))
    ci = np.flatnonzero((lon >= box['lon_min']) & (lon <= box['lon_max']))
    return ri, ci


def nearest_cells(lat, lon, point, count):
    """Nearest grid cells by Euclidean distance in degrees, original KD-tree semantics."""
    la, lo = np.meshgrid(lat, lon, indexing='ij')
    d = (lo - point[0]) ** 2 + (la - point[1]) ** 2
    flat = np.argsort(d, axis=None, kind='stable')[:count]
    raveled = d.ravel()
    return [(int(i // len(lon)), int(i % len(lon)), float(np.sqrt(raveled[i]))) for i in flat]


def nearest_valid_cells(lat, lon, point, count, mask):
    """Nearest cells restricted to the boolean mask; exact global search."""
    la, lo = np.meshgrid(lat, lon, indexing='ij')
    d = (lo - point[0]) ** 2 + (la - point[1]) ** 2
    d = np.where(mask, d, np.inf)
    flat = np.argsort(d, axis=None, kind='stable')[:count]
    raveled = d.ravel()
    return [(int(i // len(lon)), int(i % len(lon)), float(np.sqrt(raveled[i]))) for i in flat]


def decoded(var):
    value = var.astype(np.float64)
    valid = np.isfinite(value)
    for key in ('_FillValue', 'missing_value'):
        for fill in np.atleast_1d(np.asarray(var.attrs.get(key, []), dtype=np.float64)).ravel():
            if np.isfinite(fill):
                valid = valid & (value != fill)
    scale = float(np.atleast_1d(np.asarray(var.attrs.get('scale_factor', 1), dtype=np.float64)).ravel()[0])
    offset = float(np.atleast_1d(np.asarray(var.attrs.get('add_offset', 0), dtype=np.float64)).ravel()[0])
    return (value * scale + offset).where(valid)


def load_context(root, dataset):
    root = str(root)
    inputs = json.loads(open(os.path.join(root, 'matrix_inputs.json')).read())[dataset]
    preparation = json.loads(open(os.path.join(root, dataset + '_preparation.json')).read())
    reference = json.loads(open(os.path.join(root, 'tpk_reference.json')).read())
    return inputs, preparation, reference


def close_enough(observed, expected):
    if observed is None or expected is None:
        return False
    if observed['count'] != expected['count']:
        return False
    a, b = float(observed['sum']), float(expected['sum'])
    return abs(a - b) <= max(1e-6, 1e-9 * max(abs(a), abs(b)))
