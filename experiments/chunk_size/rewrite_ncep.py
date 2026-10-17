"""Rewrite the NCEP source files with alternative HDF5 chunk shapes (server, PY312).

Usage: python rewrite_ncep.py <variant>
  variant in: plane (1,94,192) | half (1,47,96) | quarter (1,24,48) | time12 (12,94,192) | time4 (4,94,192)

Reads the 40 verified NCEP sources, writes byte-identical data (same values,
same dtype, uncompressed) re-chunked, and asserts value equality per plane.
Output: /opt/mgsi-experiments/chunk_size/ncep_<variant>/Eb-free copies
plus a rewrite manifest with source/target identity.
"""
import glob
import json
import os
import sys
import tempfile
from pathlib import Path

import h5py
import numpy as np
import pyarrow.fs as pafs
from netCDF4 import Dataset

ROOT = Path(__file__).resolve().parent
VARIANTS = {'plane': (1, 94, 192), 'half': (1, 47, 96), 'quarter': (1, 24, 48),
            'time12': (12, 94, 192), 'time4': (4, 94, 192)}


def native_fs():
    parts = os.environ.get('HADOOP_CLASSPATH', os.environ.get('CLASSPATH', '')).split(':')
    os.environ['CLASSPATH'] = ':'.join(v for part in parts for v in (glob.glob(part) if '*' in part else [part]))
    return pafs.HadoopFileSystem('192.0.2.64', 9000, user='root', replication=2, extra_conf={'dfs.replication': '2'})


def read_classic(fs, path, variable):
    """NCEP/NCAR sources are NetCDF-3 classic; read via a local temp copy."""
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
            var = ds.variables[variable]
            data = np.asarray(var[:, :, :])
            attrs = {k: var.getncattr(k) for k in var.ncattrs()}
    finally:
        os.unlink(tmp_path)
    return data, attrs


def main():
    variant = sys.argv[1]
    chunks = VARIANTS[variant]
    meta = json.loads((ROOT/'physical_index.with_blocks.json').read_text())
    files = sorted(meta['files'], key=lambda f: f['path'])
    fs = native_fs()
    hdfs_dir = '/user/mgsi-experiments/chunk_size/ncep_'+variant
    out_dir = ROOT/('ncep_'+variant)
    out_dir.mkdir(exist_ok=True)
    manifest = []
    for f in files:
        data, attrs = read_classic(fs, f['path'], f['variable'])
        target = out_dir/(f['path'].replace('/', '_'))
        with h5py.File(target, 'w') as out:
            dataset = out.create_dataset(f['variable'], data=data, chunks=chunks,
                                         dtype='<f4', compression=None)
            for k, v in attrs.items():
                dataset.attrs[k] = v
        with h5py.File(target, 'r') as check:
            assert np.array_equal(np.asarray(check[f['variable']][:, :, :]), data), target
        hdfs_path = hdfs_dir+'/'+target.name
        with target.open('rb') as local, fs.open_output_stream(hdfs_path) as remote:
            remote.write(local.read())
        stored = fs.get_file_info(hdfs_path)
        manifest.append({'source': f['path'], 'target': hdfs_path,
                         'source_bytes': f['source_bytes'], 'target_bytes': stored.size,
                         'chunks': list(chunks)})
        print('REWROTE {} -> {} ({} bytes)'.format(f['path'], hdfs_path, stored.size), flush=True)
    (out_dir/'rewrite_manifest.json').write_text(json.dumps(manifest))
    print('REWRITE_READY '+json.dumps({'variant': variant, 'files': len(manifest)}), flush=True)


if __name__ == '__main__':
    main()
