"""Freeze repaired sources, verify reusable stores, rebuild native index metadata."""
from pathlib import Path
import copy
import hashlib
import json
import os
import shutil
import subprocess
import time
import urllib.parse
import urllib.request
import h5py
import numpy as np
from netCDF4 import num2date
from matrix_common import native_fs

ROOT=Path(__file__).resolve().parent
PY37='/opt/mgsi-cluster/python37/bin/python3'
PY312='/opt/mgsi-modern/python312/bin/python'

def health(path):
    url='http://192.0.2.64:50070/fsck?'+urllib.parse.urlencode({'path':path,'user.name':'root'})
    with urllib.request.urlopen(url,timeout=150) as stream: text=stream.read(16*1024**2).decode()
    assert 'Status: HEALTHY' in text or 'is HEALTHY' in text, (path,text[-3000:])
    return {'path':path,'fsck':text,'checked_at':time.time()}

def main():
    assert not (ROOT/'input_identity.json').exists(), 'Fresh input freeze required.'
    seed=json.loads((ROOT/'matrix_inputs.json').read_text())
    inputs={'gleam':copy.deepcopy(seed['gleam'])}
    fs=native_fs(); identities=[]
    for source in inputs['gleam']['files']:
        before=fs.get_file_info(source['path'])
        with fs.open_input_file(source['path']) as stream, h5py.File(stream,'r') as f:
            v=f[source['variable']]
            assert list(v.shape)==source['shape'] and np.dtype(v.dtype).str==source['dtype']
            assert list(v.chunks)==source['physical_chunks']
            assert np.array_equal(f[source['coordinate_names'][1]][:],source['lat'])
            assert np.array_equal(f[source['coordinate_names'][2]][:],source['lon'])
            td=f[source['coordinate_names'][0]]
            units=np.asarray(td.attrs['units']).flat[0]
            units=units.decode() if isinstance(units,bytes) else str(units)
            dates=num2date(td[:],units=units,calendar='standard')
            chosen=[i for i,d in enumerate(dates) if d.day==15]
            assert chosen==source['time_indices'] and len(chosen)==12
            assert len({dates[i].strftime('%Y-%m') for i in chosen})==12
        after=fs.get_file_info(source['path'])
        assert (before.size,before.mtime_ns)==(after.size,after.mtime_ns)
        source.update(source_bytes=before.size,mtime_ns=before.mtime_ns,mtime_ms=before.mtime_ns//10**6)
        identities.append({'path':source['path'],'bytes':before.size,'mtime_ns':before.mtime_ns,
                           'shape':source['shape'],'variable':source['variable']})
    (ROOT/'matrix_inputs.json').write_text(json.dumps(inputs))
    (ROOT/'input_identity.json').write_text(json.dumps(identities,indent=2))
    prep=json.loads((ROOT/'gleam_preparation.json').read_text()); assert prep['status']=='READY'
    paths=['/user/dataset/gleam',str(Path(prep['zarr']['stores'][0]).parent),prep['sedona']['path']]
    checks=[health(p) for p in paths]
    (ROOT/'input_health.json').write_text(json.dumps(checks,indent=2))
    protocol={'scope':'Matched GLEAM Eb 1981-1990; monthly 15th; 120 monthly aggregates; R1-R8',
        'configurations':['mgsi','xarray_native','xarray_zarr','sedona'],
        'worker_hosts':['192.0.2.65','192.0.2.66','192.0.2.71','192.0.2.72'],
        'flink_taskmanagers':24,'flink_subtasks':72,
        'baseline_processes':{'xarray_dask_workers':16,'sedona_executors':16},
        'memory_per_process_gib':8,
        'repetitions_per_region':{'warmup_separate':1,'measured':5},
        'query_timing':'Before plan/index selection/task-graph construction to complete monthly result return',
        'separate_costs':['engine initialization','physical metadata preparation','global index construction','format conversion'],
        'application_data_cache':'MGSI selected-plane cache cleared each query; Dask no persist or aggregate cache; Spark no persist/result cache',
        'os_hdfs_cache':'Not flushed; ordinary resident-file-cache condition disclosed; not labelled controlled cold cache',
        'disk_policy':{'author_limit_percent':90,'start_below_percent':89.80,'stop_at_percent':89.85,
                       'check_interval_seconds':2,'new_worker_files_max_bytes':512*1024**2},
        'representation_preparation':'Reused verified full-time stores; historical preparation costs identified separately, not retimed',
        'formal_scope_limit':'First GLEAM spatial batch; other datasets, temporal queries, layout sensitivity, SQL point costs and dynamic-load experiments not completed by this batch'}
    (ROOT/'protocol.json').write_text(json.dumps(protocol,indent=2))
    # This extractor reads current original HDF5 chunks and emits new offsets;
    # legacy query plans supply only their established logical layout/reference.
    subprocess.run([PY37,str(ROOT/'prepare_physical_index.py')],check=True)
    cp=':'.join(['/opt/mgsi-cluster/flink/lib/*','/app/hadoop-2.7.2/share/hadoop/common/*',
        '/app/hadoop-2.7.2/share/hadoop/common/lib/*','/app/hadoop-2.7.2/share/hadoop/hdfs/*',
        '/app/hadoop-2.7.2/share/hadoop/hdfs/lib/*'])
    classes=ROOT/'classes_mgsi'; classes.mkdir()
    subprocess.run(['/opt/mgsi-modern/jdk17/bin/javac','--release','8','-cp',cp,'-d',str(classes),
        str(ROOT/'JobManagerGlobalIndexSource.java'),str(ROOT/'ResidentMGSI.java')],check=True)
    subprocess.run(['/opt/mgsi-modern/jdk17/bin/jar','cf',str(ROOT/'resident-mgsi.jar'),'-C',str(classes),'.'],check=True)
    java=str(Path(os.environ['JAVA_HOME'])/'bin/java')
    subprocess.run([java,'-cp',str(ROOT/'resident-mgsi.jar')+':'+cp,'ResidentMGSI','blockmap',
        str(ROOT/'physical_index.json'),str(ROOT/'physical_index.with_blocks.json')],check=True)
    current=json.loads((ROOT/'physical_index.with_blocks.json').read_text())
    assert current['entries'] and len(current['files'])==len(identities)
    assert {entry['entry_id'] for entry in current['entries']}==set(range(len(current['entries'])))
    for source in identities:
        info=fs.get_file_info(source['path']); assert (info.size,info.mtime_ns)==(source['bytes'],source['mtime_ns'])
    stages=['gleam_mgsi','gleam_native','gleam_zarr','gleam_sedona']
    names=[p for p in ROOT.iterdir() if p.is_file() and (p.suffix in ('.py','.java','.jar','.sh') or p.name in
        ('matrix_inputs.json','boxes.json','gleam_reference.json','gleam_preparation.json','protocol.json',
         'physical_index.with_blocks.json','golden_results.json','input_identity.json'))]
    for label in stages:
        target=ROOT/label; target.mkdir()
        for source in names: shutil.copyfile(source,target/source.name)
    (ROOT/'preparation_complete.json').write_text(json.dumps({'status':'READY','stages':stages,
        'input_identity_sha256':hashlib.sha256((ROOT/'input_identity.json').read_bytes()).hexdigest(),
        'new_hdfs_outputs':False},indent=2))
    print('PREPARATION_READY: repaired inputs frozen, full stores healthy, current native offsets rebuilt',flush=True)

if __name__=='__main__': main()
