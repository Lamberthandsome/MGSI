"""Native HDFS access and xarray computations, with exact coordinate selection."""
import glob
import io
import os
from pathlib import Path
import socket
import time

import numpy as np
import pyarrow.fs as pafs
import xarray as xr

_FS = None


def native_fs():
    global _FS
    if _FS is None:
        parts = os.environ.get('HADOOP_CLASSPATH', os.environ.get('CLASSPATH', '')).split(':')
        os.environ['CLASSPATH'] = ':'.join(expanded for part in parts for expanded in (glob.glob(part) if '*' in part else [part]))
        _FS = pafs.HadoopFileSystem(host='192.0.2.64', port=9000, user='root')
    return _FS


def mapper(path, create=False):
    from fsspec.implementations.arrow import ArrowFSWrapper
    from fsspec.mapping import FSMap
    return FSMap(path, ArrowFSWrapper(native_fs()), check=False, create=create)


def host():
    return socket.gethostbyname(socket.gethostname())


def worker_probe():
    fs = native_fs()
    assert fs.get_file_info('/user/dataset').type == pafs.FileType.Directory
    return {'host': host(), 'pid': os.getpid(), 'python': os.sys.version.split()[0], 'xarray': xr.__version__}


def decoded(var):
    attrs = var.attrs
    value = var.astype(np.float64)
    valid = np.isfinite(value)
    for name in ('_FillValue', 'missing_value'):
        for fill in np.asarray(attrs.get(name, [])).ravel():
            if np.isfinite(fill):
                valid = valid & (value != float(fill))
    scale = float(np.asarray(attrs.get('scale_factor', 1)).ravel()[0])
    offset = float(np.asarray(attrs.get('add_offset', 0)).ravel()[0])
    return (value * scale + offset).where(valid)


def ncep_native(file, box, nonce):
    started = time.perf_counter()
    fs = native_fs()
    assert fs.get_file_info(file['path']).size == file['bytes']
    with fs.open_input_file(file['path']) as stream:
        # Bound each JNI allocation when three tasks share one worker JVM.
        blocks = []
        while True:
            block = stream.read(8 * 1024 * 1024)
            if not block:
                break
            blocks.append(block)
        content = b''.join(blocks)
    assert len(content) == file['bytes']
    with xr.open_dataset(io.BytesIO(content), engine='scipy', decode_times=False, mask_and_scale=False) as ds:
        var = ds[file['variable']]
        td, yd, xd = var.dims
        lat = np.asarray(ds[yd].values, dtype=np.float64)
        lon = np.asarray(ds[xd].values, dtype=np.float64)
        lon = np.where(lon > 180, lon - 360, lon)
        ri = np.flatnonzero((lat >= box['lat_min']) & (lat <= box['lat_max']))
        ci = np.flatnonzero((lon >= box['lon_min']) & (lon <= box['lon_max']))
        selected = decoded(var.isel({yd: ri, xd: ci}))
        sums = selected.sum(dim=(yd, xd), skipna=True, dtype=np.float64).values
        counts = selected.count(dim=(yd, xd)).values
        monthly = {label[:7]: {'sum': float(total), 'count': int(count)} for label, total, count in zip(file['times'], sums, counts)}
    return {'host': host(), 'pid': os.getpid(), 'path': file['path'], 'monthly': monthly,
            'read_bytes': len(content), 'worker_seconds': time.perf_counter()-started}


class CountingReader(io.BufferedIOBase):
    def __init__(self, stream):
        self.stream, self.bytes_read = stream, 0

    def read(self, n=-1):
        value = self.stream.read(n)
        self.bytes_read += len(value)
        return value

    def readinto(self, target):
        value = self.read(len(target))
        target[:len(value)] = value
        return len(value)

    def seek(self, offset, whence=0):
        return self.stream.seek(offset, whence)

    def tell(self):
        return self.stream.tell()

    def readable(self):
        return True

    def seekable(self):
        return True


def gleam_native(file, extent, box, nonce):
    started = time.perf_counter()
    fs = native_fs()
    assert fs.get_file_info(file['path']).size == file['source_bytes']
    with fs.open_input_file(file['path']) as stream:
        reader = CountingReader(stream)
        with xr.open_dataset(reader, engine='h5netcdf', decode_times=False, mask_and_scale=False, cache=False) as ds:
            td, yd, xd = ds[file['variable']].dims
            r0, r1, c0, c1 = extent
            lat = np.asarray(ds[yd].values, dtype=np.float64)
            lon = np.asarray(ds[xd].values, dtype=np.float64)
            ri = np.flatnonzero((lat[r0:r1] >= box['lat_min']) & (lat[r0:r1] <= box['lat_max'])) + r0
            ci = np.flatnonzero((lon[c0:c1] >= box['lon_min']) & (lon[c0:c1] <= box['lon_max'])) + c0
            if not len(ri) or not len(ci):
                monthly = {}
            else:
                raw = ds[file['variable']].isel({td: file['time_indices'], yd: ri, xd: ci})
                value = decoded(raw)
                sums = value.sum(dim=(yd, xd), skipna=True, dtype=np.float64).values
                counts = value.count(dim=(yd, xd)).values
                monthly = {label[:7]: {'sum': float(total), 'count': int(count)} for label, total, count in zip(file['time_labels'], sums, counts)}
    return {'host': host(), 'pid': os.getpid(), 'path': file['path'], 'extent': extent, 'monthly': monthly,
            'read_bytes': reader.bytes_read, 'worker_seconds': time.perf_counter()-started}


def merge(rows):
    monthly = {}
    for row in rows:
        for month, value in row['monthly'].items():
            target = monthly.setdefault(month, {'sum': 0.0, 'count': 0})
            target['sum'] += value['sum']
            target['count'] += value['count']
    for value in monthly.values():
        value['mean'] = value['sum']/value['count'] if value['count'] else None
    return monthly
