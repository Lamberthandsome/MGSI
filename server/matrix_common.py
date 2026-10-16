"""Four archive adapters. Sources and native grid centers are never modified."""
import datetime as dt
import glob
import hashlib
import io
import json
import math
import os
from pathlib import Path
import socket
import time
import zlib

import h5py
import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import rasterio
from rasterio.io import MemoryFile
from rasterio.transform import Affine
import xarray as xr
from numcodecs import Blosc, Shuffle, blosc
blosc.set_nthreads(1)

_FS = None


def native_fs():
    global _FS
    if _FS is None:
        parts = os.environ.get('HADOOP_CLASSPATH', os.environ.get('CLASSPATH', '')).split(':')
        os.environ['CLASSPATH'] = ':'.join(v for part in parts for v in (glob.glob(part) if '*' in part else [part]))
        # Only experiment-created files get two replicas. Original archives are untouched.
        _FS = pafs.HadoopFileSystem('192.0.2.64', 9000, user='root', replication=2, extra_conf={'dfs.replication': '2'})
    return _FS


def mapper(path):
    from fsspec.implementations.arrow import ArrowFSWrapper
    from fsspec.mapping import FSMap
    return FSMap(path, ArrowFSWrapper(native_fs()), check=False)


def host():
    return socket.gethostbyname(socket.gethostname())


def probe():
    assert native_fs().get_file_info('/user/dataset').type == pafs.FileType.Directory
    return {'host': host(), 'pid': os.getpid(), 'python': os.sys.version.split()[0]}


def numeric(value):
    return np.asarray(value, dtype=np.float64)


def decoded(var):
    value = var.astype(np.float64)
    valid = np.isfinite(value)
    for key in ('_FillValue', 'missing_value'):
        for fill in numeric(var.attrs.get(key, [])).ravel():
            if np.isfinite(fill):
                valid = valid & (value != fill)
    scale = float(numeric(var.attrs.get('scale_factor', 1)).ravel()[0])
    offset = float(numeric(var.attrs.get('add_offset', 0)).ravel()[0])
    return (value * scale + offset).where(valid)


def decode_numpy(raw, attrs):
    values = raw.astype(np.float64)
    valid = np.isfinite(values)
    for key in ('_FillValue', 'missing_value'):
        for missing in numeric(attrs.get(key, [])).ravel():
            if np.isfinite(missing):
                valid &= values != missing
    values *= float(numeric(attrs.get('scale_factor', 1)).ravel()[0])
    values += float(numeric(attrs.get('add_offset', 0)).ravel()[0])
    return values, valid


def add_month(target, month, total, count):
    value = target.setdefault(month, {'sum': 0.0, 'count': 0})
    value['sum'] += float(total)
    value['count'] += int(count)


def merge(rows):
    result = {}
    for row in rows:
        for month, value in row['monthly'].items():
            add_month(result, month, value['sum'], value['count'])
    for value in result.values():
        value['mean'] = value['sum']/value['count'] if value['count'] else None
    return result


class CountingReader(io.BufferedIOBase):
    def __init__(self, source):
        self.source, self.bytes_read = source, 0

    def read(self, n=-1):
        value = self.source.read(n)
        self.bytes_read += len(value)
        return value

    def readinto(self, target):
        value = self.read(len(target))
        target[:len(value)] = value
        return len(value)

    def seek(self, offset, whence=0):
        return self.source.seek(offset, whence)

    def tell(self):
        return self.source.tell()

    def readable(self):
        return True

    def seekable(self):
        return True


def read_bytes(path):
    blocks = []
    with native_fs().open_input_file(path) as stream:
        while True:
            block = stream.read(8*1024*1024)
            if not block:
                break
            blocks.append(block)
    return b''.join(blocks)


def hdf_native(file, extent, box, nonce):
    started = time.perf_counter()
    fs = native_fs()
    assert fs.get_file_info(file['path']).size == file['source_bytes']
    r0, r1, c0, c1 = extent
    monthly = {}
    with fs.open_input_file(file['path']) as stream:
        reader = CountingReader(stream)
        with xr.open_dataset(reader, engine='h5netcdf', mask_and_scale=False, decode_times=False, cache=False) as ds:
            var = ds[file['variable']]
            td, yd, xd = var.dims
            lat = np.asarray(file['lat'])[r0:r1]
            lon = np.asarray(file['lon'])[c0:c1]
            ri = np.flatnonzero((lat >= box['lat_min']) & (lat <= box['lat_max']))+r0
            ci = np.flatnonzero((lon >= box['lon_min']) & (lon <= box['lon_max']))+c0
            if len(ri) and len(ci):
                value = decoded(var.isel({td: file['time_indices'], yd: ri, xd: ci}))
                sums = value.sum(dim=(yd, xd), skipna=True, dtype=np.float64).values
                counts = value.count(dim=(yd, xd)).values
                for label, total, count in zip(file['time_labels'], sums, counts):
                    # ECMWF has 24 observations on every selected day; keep them all.
                    add_month(monthly, label[:7], total, count)
    return {'host': host(), 'path': file['path'], 'extent': extent, 'monthly': monthly,
            'read_bytes': reader.bytes_read, 'worker_seconds': time.perf_counter()-started}


def chirps_native(file, box, nonce):
    started = time.perf_counter()
    payload = read_bytes(file['path'])
    assert len(payload) == file['bytes']
    with MemoryFile(payload) as memory, memory.open() as raster:
        lat = raster.transform.f+(np.arange(raster.height)+0.5)*raster.transform.e
        lon = raster.transform.c+(np.arange(raster.width)+0.5)*raster.transform.a
        ri = np.flatnonzero((lat >= box['lat_min']) & (lat <= box['lat_max']))
        ci = np.flatnonzero((lon >= box['lon_min']) & (lon <= box['lon_max']))
        window = rasterio.windows.Window(int(ci[0]), int(ri[0]), len(ci), len(ri))
        array = xr.DataArray(raster.read(1, window=window), dims=('lat','lon'),
            attrs={'missing_value': -9999, 'scale_factor': raster.scales[0], 'add_offset': raster.offsets[0]})
        value = decoded(array)
        total, count = float(value.sum(skipna=True, dtype=np.float64)), int(value.count())
    return {'host': host(), 'path': file['path'], 'monthly': {file['month']: {'sum': total, 'count': count}},
            'read_bytes': len(payload), 'worker_seconds': time.perf_counter()-started}


RASTER_SCHEMA = pa.schema([
    ('date', pa.string()), ('day', pa.int32()), ('months', pa.list_(pa.string())),
    ('lat_min', pa.float64()), ('lat_max', pa.float64()), ('lon_min', pa.float64()), ('lon_max', pa.float64()),
    ('nodata', pa.float64()), ('content', pa.binary())])


def encode_raster(values, valid, lat, lon, dx, dy):
    # Every pixel keeps its original value; only row/column order is normalized.
    yi, xi = np.argsort(-lat), np.argsort(lon)
    values = values[:, yi][:, :, xi]
    valid = valid[:, yi][:, :, xi]
    dtype = np.float32 if np.array_equal(values[valid], values[valid].astype(np.float32).astype(np.float64)) else np.float64
    nodata = -float(np.finfo(dtype).max)
    assert not np.any(values[valid] == nodata)
    array = np.where(valid, values, nodata).astype(dtype)
    transform = Affine(dx, 0, float(lon[xi][0])-dx/2, 0, -dy, float(lat[yi][0])+dy/2)
    with MemoryFile() as memory:
        with memory.open(driver='GTiff', height=len(lat), width=len(lon), count=len(array), dtype=np.dtype(dtype).name,
                         crs='EPSG:4326', transform=transform, nodata=nodata, compress='deflate', zlevel=3) as raster:
            raster.write(array)
        payload = memory.read()
    return payload, nodata, array


def assert_centers(lat, lon, dx, dy, boxes):
    # Float32 NetCDF coordinates can differ slightly from an affine grid. Verify
    # the actual requested center selections instead of resampling the values.
    affine_lat = float(np.max(lat))-np.arange(len(lat))*dy
    affine_lon = float(np.min(lon))+np.arange(len(lon))*dx
    source_lat, source_lon = np.sort(lat)[::-1], np.sort(lon)
    for box in boxes.values():
        assert np.array_equal((source_lat >= box['lat_min']) & (source_lat <= box['lat_max']),
                              (affine_lat >= box['lat_min']-1e-8) & (affine_lat <= box['lat_max']+1e-8))
        assert np.array_equal((source_lon >= box['lon_min']) & (source_lon <= box['lon_max']),
                              (affine_lon >= box['lon_min']-1e-8) & (affine_lon <= box['lon_max']+1e-8))
    return float(max(np.max(np.abs(source_lat-affine_lat)), np.max(np.abs(source_lon-affine_lon))))


def clone_and_raster_tile(file, extent, zpath, raster_root, boxes, layout='native'):
    """One source read prepares full-time Zarr and full-time Sedona raster tiles."""
    started = time.perf_counter()
    fs = native_fs()
    before = fs.get_file_info(file['path'])
    assert (before.size, before.mtime_ns) == (file['source_bytes'], file['mtime_ns'])
    r0, r1, c0, c1 = extent
    nt = file['shape'][0]
    raw = np.full((nt, r1-r0, c1-c0), np.nan, dtype=file['dtype'])
    chunks, digests, source_digests, compressed_bytes = tuple(file['physical_chunks']), [], [], 0
    with fs.open_input_file(file['path']) as source, h5py.File(source, 'r', rdcc_nbytes=32*1024*1024) as data:
        field = data[file['variable']]
        plist = field.id.get_create_plist()
        filters = [plist.get_filter(i)[0] for i in range(plist.get_nfilters())]
        assert filters == ([2, 1] if file['shuffle'] else [1]), filters
        for t0 in range(0, nt, chunks[0]):
            for y0 in range(r0, r1, chunks[1]):
                for x0 in range(c0, c1, chunks[2]):
                    mask, payload = field.id.read_direct_chunk((t0,y0,x0))
                    assert mask == 0, ('Skipped HDF5 filter', file['path'], t0,y0,x0,mask)
                    key = '{}.{}.{}'.format(t0//chunks[0],y0//chunks[1],x0//chunks[2])
                    entry={'key': key, 'bytes': len(payload), 'sha256': hashlib.sha256(payload).hexdigest()}
                    source_digests.append(entry)
                    if layout=='native':
                        with fs.open_output_stream(zpath+'/value/'+key) as target:
                            target.write(payload)
                        digests.append(entry)
                        compressed_bytes += len(payload)
                    decoded_bytes = zlib.decompress(payload)
                    if file['shuffle']:
                        decoded_bytes = Shuffle(np.dtype(file['dtype']).itemsize).decode(decoded_bytes)
                    block = np.frombuffer(decoded_bytes, dtype=file['dtype']).reshape(chunks)
                    t1, y1, x1 = min(nt,t0+chunks[0]), min(r1,y0+chunks[1]), min(c1,x0+chunks[2])
                    raw[t0:t1,y0-r0:y1-r0,x0-c0:x1-c0] = block[:t1-t0,:y1-y0,:x1-x0]
        # Check both interior and padded edge chunks against the independent HDF5 reader.
        for ti in (0, nt//2, nt-1):
            observed = field[ti,r0:r1,c0:c1]
            assert np.array_equal(raw[ti], observed, equal_nan=True), (file['path'],extent,ti)
    lat, lon = np.asarray(file['lat'])[r0:r1], np.asarray(file['lon'])[c0:c1]
    dx = float((max(file['lon'])-min(file['lon']))/(len(file['lon'])-1))
    dy = float((max(file['lat'])-min(file['lat']))/(len(file['lat'])-1))
    xi = np.argsort(lon)
    # A tile crossing longitude 180 must become two contiguous rasters.
    lon_groups = np.split(xi, np.flatnonzero(np.diff(lon[xi]) > dx*1.5)+1)
    first = dt.datetime.fromisoformat(file['first_time'])
    hourly = file['shape'][0] > 1000
    step, monthly = (24 if hourly else 1), {region: {} for region in boxes}
    import zarr
    array=zarr.open_array(mapper(zpath+'/value'),mode='r')
    if layout=='daily':
        output_chunks=tuple(file['output_chunks'])
        assert output_chunks[0]==step and r0%output_chunks[1]==c0%output_chunks[2]==0
        compressor=Blosc(cname='zstd',clevel=3,shuffle=Blosc.BITSHUFFLE)
    writers, streams, raster_paths = [], [], []
    errors = []
    for part, cols in enumerate(lon_groups):
        errors.append(assert_centers(lat,lon[cols],dx,dy,boxes))
        path = raster_root+'/{}_{}_{}_{}.parquet'.format(first.year,r0,c0,part)
        stream = fs.open_output_stream(path)
        streams.append(stream)
        writers.append(pq.ParquetWriter(stream, RASTER_SCHEMA, compression='NONE'))
        raster_paths.append(path)
    digest = hashlib.sha256()
    for t0 in range(0, nt, step):
        if layout=='daily':
            # Complete daily chunks are owned by exactly one spatial task. Edge
            # padding uses the dataset fill; no original observation is omitted.
            block=np.full(output_chunks,array.fill_value,dtype=file['dtype'])
            block[:min(step,nt-t0),:r1-r0,:c1-c0]=raw[t0:t0+step]
            payload=compressor.encode(block)
            key='{}.{}.{}'.format(t0//step,r0//output_chunks[1],c0//output_chunks[2])
            with fs.open_output_stream(zpath+'/value/'+key) as target: target.write(payload)
            digests.append({'key':key,'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest()})
            compressed_bytes+=len(payload)
        times = [first+dt.timedelta(hours=t) if hourly else first+dt.timedelta(days=t) for t in range(t0,min(nt,t0+step))]
        assert all(v.date() == times[0].date() for v in times)
        values, valid = decode_numpy(raw[t0:t0+step],file['attrs'])
        if times[0].day == 15:
            for region, box in boxes.items():
                yi = np.flatnonzero((lat>=box['lat_min']) & (lat<=box['lat_max']))
                xsel = np.flatnonzero((lon>=box['lon_min']) & (lon<=box['lon_max']))
                if len(yi) and len(xsel):
                    v = values[:,yi][:,:,xsel]
                    m = valid[:,yi][:,:,xsel]
                    add_month(monthly[region],times[0].strftime('%Y-%m'),np.where(m,v,0).sum(dtype=np.float64),m.sum())
        for part, cols in enumerate(lon_groups):
            payload, nodata, expected = encode_raster(values[:,:,cols],valid[:,:,cols],lat,lon[cols],dx,dy)
            digest.update(hashlib.sha256(payload).digest())
            if times[0].day == 15 or t0 in (0,nt-step):
                with MemoryFile(payload) as memory, memory.open() as raster:
                    assert np.array_equal(raster.read(),expected,equal_nan=True)
            row = {'date': times[0].date().isoformat(), 'day': times[0].day, 'months': [v.strftime('%Y-%m') for v in times],
                   'lat_min': float(lat.min()), 'lat_max': float(lat.max()), 'lon_min': float(lon[cols].min()),
                   'lon_max': float(lon[cols].max()), 'nodata': nodata, 'content': payload}
            writers[part].write_table(pa.Table.from_pylist([row],schema=RASTER_SCHEMA),row_group_size=1)
    for writer in writers:
        writer.close()
    for stream in streams:
        stream.close()
    if (r0,c0)==(0,0) or (r1,c1)==tuple(file['shape'][1:]):
        for ti in (0,nt//2,nt-1):
            assert np.array_equal(array[ti,r0:r1,c0:c1],raw[ti],equal_nan=True), ('Zarr codec',file['path'],extent,ti)
    after = fs.get_file_info(file['path'])
    assert (before.size,before.mtime_ns)==(after.size,after.mtime_ns)
    return {'host': host(), 'path': file['path'], 'extent': extent, 'monthly': monthly,
        'zarr_chunks': digests, 'zarr_compressed_bytes': compressed_bytes,
        'source_chunks':source_digests, 'zarr_layout':layout,
        'raster_files': [{'path':p,'bytes':fs.get_file_info(p).size} for p in raster_paths],
        'raster_rows': len(range(0,nt,step))*len(lon_groups), 'raster_content_hash': digest.hexdigest(),
        'coordinate_max_affine_error_degrees': max(errors), 'seconds': time.perf_counter()-started}


def chirps_to_zarr(file, time_index, path, lat, lon):
    import zarr
    started = time.perf_counter()
    before = native_fs().get_file_info(file['path'])
    assert (before.size,before.mtime_ns)==(file['bytes'],file['mtime_ns'])
    payload = read_bytes(file['path'])
    with MemoryFile(payload) as memory, memory.open() as raster:
        assert raster.count==1 and raster.shape==(len(lat),len(lon))
        actual_lat=raster.transform.f+(np.arange(raster.height)+0.5)*raster.transform.e
        actual_lon=raster.transform.c+(np.arange(raster.width)+0.5)*raster.transform.a
        assert np.array_equal(actual_lat,lat) and np.array_equal(actual_lon,lon)
        raw=raster.read(1)
        array=zarr.open_array(mapper(path+'/value'),mode='r+')
        array[time_index,:,:]=raw
        if file['path'].endswith('.15.tif'):
            assert np.array_equal(array[time_index,:,:],raw,equal_nan=True)
    after=native_fs().get_file_info(file['path'])
    assert (before.size,before.mtime_ns)==(after.size,after.mtime_ns)
    return {'path':file['path'],'source_sha256':hashlib.sha256(payload).hexdigest(),
        'host':host(),'seconds':time.perf_counter()-started}
