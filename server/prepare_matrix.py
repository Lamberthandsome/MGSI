"""Prepare query-independent full archives. All preprocessing has separate timing."""
import argparse
import datetime as dt
import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import subprocess
import time
import urllib.request

from distributed import Client, as_completed
import h5py
import netCDF4
import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import zarr
from numcodecs import Blosc, Shuffle, Zlib
from rasterio.io import MemoryFile

from matrix_common import (native_fs, mapper, clone_and_raster_tile, chirps_to_zarr,
                           RASTER_SCHEMA, encode_raster, merge, host)

ROOT=Path(__file__).resolve().parent
OLD=Path('/opt/mgsi-experiments/20261007-modern-baselines')
HDFS='/mgsi-experiments/20261007-modern-matrix/'+ROOT.name


def write(name, value):
    (ROOT/name).write_text(json.dumps(value,indent=2,allow_nan=False))


def validate(actual, expected):
    assert actual.keys()==expected.keys() and len(actual)==120
    for month, a in actual.items():
        e=expected[month]
        assert a['count']==e['count'],(month,'count',a,e)
        assert math.isclose(a['sum'],e['sum'],rel_tol=1e-9,abs_tol=1e-6),(month,'sum',a,e)


def coordinate(group, name, values):
    values=np.asarray(values)
    array=group.create_dataset(name,data=values,chunks=values.shape,compressor=None)
    array.attrs['_ARRAY_DIMENSIONS']=[name]


def all_dates(file):
    first=dt.datetime.fromisoformat(file['first_time'])
    hourly=file['shape'][0]>1000
    dates=np.asarray([(first+(dt.timedelta(hours=i) if hourly else dt.timedelta(days=i))).isoformat()
                      for i in range(file['shape'][0])])
    assert dates[-1]==file['last_time']
    assert dates[np.asarray(file['time_indices'])].tolist()==file['time_labels']
    return dates


def setup():
    assert not (ROOT/'matrix_inputs.json').exists()
    received=json.loads((ROOT/'received_files.json').read_text())
    headers=json.loads((ROOT/'hdf5_sources.json').read_text())
    # Hadoop 2.7 libhdfs reports whole seconds; WebHDFS preserves milliseconds.
    for folder,files in received.items():
        with urllib.request.urlopen('http://192.0.2.64:50070/webhdfs/v1/user/dataset/'+folder+'?op=LISTSTATUS&user.name=root',timeout=60) as stream:
            statuses=json.load(stream)['FileStatuses']['FileStatus']
        identities={v['pathSuffix']:v for v in statuses if v['type']=='FILE'}
        for file in files:
            exact=identities[Path(file['path']).name]
            assert exact['length']==file['bytes'] and exact['modificationTime']//1000==file['mtime_ns']//1000000000
            file['mtime_ms']=exact['modificationTime']
    write('received_files.json',received)
    for name,source in headers.items():
        identities={v['path']:v for v in received[name]}
        for file in source['files']: file['mtime_ms']=identities[file['path']]['mtime_ms']
    for name in ('boxes.json','ncep_reference.json','chirps_reference.json','gleam_reference.json',
                 'ncep_input_manifest.json','chirps_input_manifest.json','zarr_input_manifest.json',
                 'portable_runtime_manifest.json','runtime_manifest.json','modern_freeze.txt'):
        shutil.copyfile(OLD/name,ROOT/name)
    inputs={}
    for name, folder in (('ncep','ncepncar'),('chirps','chirps')):
        source=json.loads((ROOT/(name+'_input_manifest.json')).read_text())
        identities={f['path']:f for f in received[folder]}
        for file in source['files']:
            fresh=identities[file['path']]
            assert file['bytes']==fresh['bytes'] and file['mtime']==fresh['mtime_ms']
            file['mtime_ns']=fresh['mtime_ns']
            file['mtime_ms']=fresh['mtime_ms']
        inputs[name]=source
    for name, source in headers.items():
        source['scope']=('Full {}-{} annual {} archive, target variable {}; '
                         'all observations on every monthly 15th, 120 monthly sum/count/mean.').format(
            source['first_year'],source['last_year'],name,source['files'][0]['variable'])
        inputs[name]=source
    write('matrix_inputs.json',inputs)
    write('protocol.json',{
        'datasets':['ncep','chirps','gleam','ecmwf'],
        'methods':['sedona','xarray_native','xarray_zarr'],
        'regions':list(range(1,9)), 'repeats':3,
        'time_query':'Monthly 15th, all native observations; NCEP uses all four UTC monthly products.',
        'result':'120 monthly sums, valid counts and means; exact counts, sum rtol=1e-9/atol=1e-6.',
        'query_compute':'Four workers, 24 processes/executors x 3 threads/cores x 8GiB; 72 total.',
        'initialization':'Resident engines; startup and all source/format/index preparation measured separately.',
        'caches':'No result cache or persist; OS/HDFS caches uncontrolled. Repeat 1 separate from later repeats.',
        'preparation':'Only target variables, full original global coordinates and full original time coverage; no region or day-15 subset stores.',
        'sedona_netcdf':'Full-time, full-grid lossless GeoTIFF raster tiles in Parquet; pushdown day and tile bounds. Gaussian NCEP latitudes remain individual native-center rows.',
        'zarr_netcdf':'Full Zarr v2 native values and coordinate centers; chunks daily (GLEAM) or 24 hourly observations (ECMWF) by source-aligned spatial tiles, Blosc/Zstd3/bitshuffle; no time or region subset.',
        'zarr_chirps':'All 3652 daily full-grid rasters, float32, chunks (1,500,1440), Blosc/Zstd3.',
        'storage':'New experiment files replication 2, originals unchanged; stop scheduling preparation below 350GiB free.',
        'scope_limit':'Pilot matrix of new baselines; formal MGSI comparison and remaining reviewer experiments are separate work.'})
    print('MATRIX_PROTOCOL_READY',flush=True)


def space_guard():
    output=subprocess.check_output(['/app/hadoop-2.7.2/bin/hdfs','dfsadmin','-report'],text=True)
    remaining=int(re.search(r'^DFS Remaining: (\d+)',output,re.M).group(1))
    write('storage_latest.json',{'checked_at':dt.datetime.now().isoformat(),'remaining_bytes':remaining})
    assert remaining>350*1024**3,('Preparation storage guard: retain >=350GiB free',remaining)
    return remaining


def prepare_ncep():
    started=time.perf_counter()
    fs=native_fs()
    root=HDFS+'/sedona_ncep_all_monthly'
    assert fs.get_file_info(root).type==pafs.FileType.NotFound
    fs.create_dir(root,recursive=True)
    data=np.load(OLD/'ncep_all_native_values.npz')
    values,lat,lon,times=(data[k] for k in ('value','lat','lon','time'))
    inputs=json.loads((ROOT/'matrix_inputs.json').read_text())['ncep']
    files=[]
    for file in inputs['files']:
        path=root+'/'+Path(file['path']).stem+'.parquet'
        indices=np.asarray([np.flatnonzero(times==label)[0] for label in file['times']])
        with fs.open_output_stream(path) as stream, pq.ParquetWriter(stream,RASTER_SCHEMA,compression='NONE') as writer:
            for ti in indices:
                rows=[]
                for yi in range(len(lat)):
                    raw=values[ti:ti+1,yi:yi+1,:]
                    payload,nodata,expected=encode_raster(raw,np.isfinite(raw),lat[yi:yi+1],lon,1.875,1.0)
                    with MemoryFile(payload) as memory,memory.open() as raster:
                        assert np.array_equal(raster.read(),expected,equal_nan=True)
                    rows.append({'date':str(times[ti])[:10],'day':0,'months':[str(times[ti])[:7]],
                        'lat_min':float(lat[yi]),'lat_max':float(lat[yi]),'lon_min':float(lon.min()),'lon_max':float(lon.max()),
                        'nodata':nodata,'content':payload})
                writer.write_table(pa.Table.from_pylist(rows,schema=RASTER_SCHEMA),row_group_size=94)
        files.append({'path':path,'bytes':fs.get_file_info(path).size})
    zarr_input=json.loads((ROOT/'zarr_input_manifest.json').read_text())
    receipt={'status':'READY','dataset':'ncep','seconds_separate':time.perf_counter()-started,
        'sedona':{'path':root,'files':files,'bytes':sum(f['bytes'] for f in files),'rows':480*94},
        'zarr':{'stores':[zarr_input['path']],'reuse_existing_complete_input':zarr_input},
        'source_values':'Reused independently decoded 480x94x192 full native array; original file identity checked; original extraction preparation has separate old receipt.'}
    write('ncep_preparation.json',receipt)
    print('NCEP_FULL_RASTER_PREPARATION_PASS '+json.dumps({'seconds':receipt['seconds_separate'],'bytes':receipt['sedona']['bytes']}),flush=True)


def prepare_hdf(client,name):
    started=time.perf_counter()
    inputs=json.loads((ROOT/'matrix_inputs.json').read_text())[name]
    boxes=json.loads((ROOT/'boxes.json').read_text())
    fs=native_fs()
    raster_root=HDFS+'/sedona_'+name+'_all_time'
    assert fs.get_file_info(raster_root).type==pafs.FileType.NotFound
    fs.create_dir(raster_root,recursive=True)
    stores, jobs, expected_chunks, expected_source_chunks, source_chunk_count=[],[],0,0,0
    for file in inputs['files']:
        year=dt.datetime.fromisoformat(file['first_time']).year
        zpath=HDFS+'/zarr_'+name+'_all_time/'+str(year)
        assert fs.get_file_info(zpath).type==pafs.FileType.NotFound
        group=zarr.open_group(mapper(zpath),mode='w')
        chunks=tuple(file['physical_chunks'])
        with fs.open_input_file(file['path']) as stream, h5py.File(stream,'r') as data:
            field=data[file['variable']]
            timevar=data[file['coordinate_names'][0]]
            units=timevar.attrs['units']
            if isinstance(units,bytes): units=units.decode()
            native_dates=netCDF4.num2date(timevar[:],units=units,only_use_cftime_datetimes=False)
            assert [v.isoformat() for v in native_dates]==all_dates(file).tolist()
            count=math.prod(math.ceil(n/c) for n,c in zip(field.shape,field.chunks))
            assert field.id.get_num_chunks()==count,('Sparse chunks need explicit fill handling',file['path'])
            fill=field.fillvalue
        height,width=(chunks[1]*2,chunks[2]*2) if name=='ecmwf' else (chunks[1]*4,chunks[2]*4)
        output_chunks=(24 if name=='ecmwf' else 1,height,width)
        file['output_chunks']=list(output_chunks)
        expected_source_chunks+=count
        expected_chunks+=math.prod(math.ceil(n/c) for n,c in zip(file['shape'],output_chunks))
        array=group.create_dataset('value',shape=tuple(file['shape']),dtype=file['dtype'],chunks=output_chunks,
            compressor=Blosc(cname='zstd',clevel=3,shuffle=Blosc.BITSHUFFLE),
            fill_value=fill)
        array.attrs.update({k:v for k,v in file['attrs'].items() if k in ('units','long_name','standard_name','missing_value','scale_factor','add_offset')})
        if '_FillValue' in file['attrs']:
            array.attrs['missing_value']=file['attrs']['_FillValue']
        array.attrs['_ARRAY_DIMENSIONS']=['time','lat','lon']
        coordinate(group,'lat',file['lat'])
        coordinate(group,'lon',file['lon'])
        coordinate(group,'time',all_dates(file))
        group.attrs.update({'source':file['path'],'variable':file['variable'],'complete_original_time_and_space':True})
        zarr.consolidate_metadata(mapper(zpath))
        stores.append(zpath)
        for y0 in range(0,file['shape'][1],height):
            for x0 in range(0,file['shape'][2],width):
                extent=(y0,min(y0+height,file['shape'][1]),x0,min(x0+width,file['shape'][2]))
                jobs.append((file,extent,zpath,raster_root,boxes,'daily'))
    futures=[client.submit(clone_and_raster_tile,*job,pure=False,retries=0) for job in jobs]
    totals={region:{} for region in boxes}
    raster_files, chunk_count, compressed_bytes, rows_count=[],0,0,0
    digest=hashlib.sha256()
    with gzip.open(ROOT/(name+'_preparation_records.jsonl.gz'),'wt') as log:
        for index,future in enumerate(as_completed(futures),1):
            row=future.result()
            log.write(json.dumps(row,allow_nan=False)+'\n')
            log.flush()
            future.release()
            raster_files.extend(row['raster_files'])
            chunk_count+=len(row['zarr_chunks'])
            source_chunk_count+=len(row['source_chunks'])
            compressed_bytes+=row['zarr_compressed_bytes']
            rows_count+=row['raster_rows']
            for item in row['zarr_chunks']:
                digest.update(json.dumps([row['path'],item],sort_keys=True).encode())
            for region in boxes:
                totals[region]=merge([{'monthly':totals[region]},{'monthly':row['monthly'][region]}])
            write(name+'_preparation_progress.json',{'status':'RUNNING','completed_tiles':index,'total_tiles':len(jobs),
                'seconds':time.perf_counter()-started,'zarr_compressed_bytes':compressed_bytes,
                'raster_bytes':sum(f['bytes'] for f in raster_files)})
            if index%20==0:
                space_guard()
                print('FULL_ARCHIVE_PROGRESS '+json.dumps({'dataset':name,'tiles':index,'total':len(jobs),'seconds':time.perf_counter()-started}),flush=True)
    assert chunk_count==expected_chunks,(chunk_count,expected_chunks)
    assert source_chunk_count==expected_source_chunks,(source_chunk_count,expected_source_chunks)
    for region in boxes:
        assert len(totals[region])==120
    if name=='gleam':
        reference=json.loads((ROOT/'gleam_reference.json').read_text())
        for region in boxes:
            validate(totals[region],reference[region])
    else:
        write(name+'_reference.json',totals)
    receipt={'status':'READY','dataset':name,'seconds_separate':time.perf_counter()-started,'tiles':len(jobs),
        'zarr':{'stores':stores,'data_chunks':chunk_count,'compressed_chunk_bytes':compressed_bytes,
                 'complete_chunk_digest_in_completion_order':digest.hexdigest(),'preserves_original_physical_chunks':False,
                 'source_physical_chunks_verified':source_chunk_count,'logical_chunk_layout':'Full daily/hourly-24 array with source-aligned spatial tiles; Blosc/Zstd3/bitshuffle'},
        'sedona':{'path':raster_root,'files':raster_files,'bytes':sum(f['bytes'] for f in raster_files),'rows':rows_count},
        'validation':'All chunk values reconstructed; HDF5 interior/edge samples exact; Zarr codec samples exact; selected-day GeoTIFF roundtrip exact; original-coordinate 960 monthly aggregates checked.'}
    write(name+'_preparation.json',receipt)
    print('FULL_ARCHIVE_PREPARATION_PASS '+json.dumps({'dataset':name,'seconds':receipt['seconds_separate'],'chunks':chunk_count,'raster_bytes':receipt['sedona']['bytes']}),flush=True)


def prepare_chirps(client):
    started=time.perf_counter()
    fs=native_fs()
    files=sorted(json.loads((ROOT/'received_files.json').read_text())['chirps'],key=lambda v:v['path'])
    path=HDFS+'/zarr_chirps_all_days'
    assert fs.get_file_info(path).type==pafs.FileType.NotFound
    payload=None
    from matrix_common import read_bytes
    payload=read_bytes(files[0]['path'])
    with MemoryFile(payload) as memory,memory.open() as raster:
        lat=raster.transform.f+(np.arange(raster.height)+0.5)*raster.transform.e
        lon=raster.transform.c+(np.arange(raster.width)+0.5)*raster.transform.a
        assert raster.scales==(1.0,) and raster.offsets==(0.0,)
    group=zarr.open_group(mapper(path),mode='w')
    value=group.create_dataset('value',shape=(len(files),len(lat),len(lon)),dtype='<f4',chunks=(1,500,1440),
                               compressor=Blosc(cname='zstd',clevel=3,shuffle=Blosc.BITSHUFFLE),fill_value=np.nan)
    value.attrs.update({'_ARRAY_DIMENSIONS':['time','lat','lon'],'missing_value':-9999,'units':'mm'})
    labels=[]
    for file in files:
        year,month,day=re.search(r'chirps-v2[.]0[.](\d{4})[.](\d{2})[.](\d{2})[.]tif$',file['path']).groups()
        labels.append(year+'-'+month+'-'+day+'T00:00:00')
    assert len(set(labels))==3652
    assert [dt.datetime.fromisoformat(v).date() for v in labels]==[
        dt.date(1981,1,1)+dt.timedelta(days=i) for i in range(3652)]
    coordinate(group,'time',labels)
    coordinate(group,'lat',lat)
    coordinate(group,'lon',lon)
    zarr.consolidate_metadata(mapper(path))
    futures=[client.submit(chirps_to_zarr,file,i,path,lat,lon,pure=False,retries=0) for i,file in enumerate(files)]
    with (ROOT/'chirps_preparation_records.jsonl').open('w') as log:
        for i,future in enumerate(as_completed(futures),1):
            row=future.result()
            log.write(json.dumps(row)+'\n'); log.flush()
            future.release()
            if i%100==0:
                space_guard()
                write('chirps_preparation_progress.json',{'status':'RUNNING','files_complete':i,'total':3652,'seconds':time.perf_counter()-started})
                print('FULL_ARCHIVE_PROGRESS '+json.dumps({'dataset':'chirps','files':i,'total':3652,'seconds':time.perf_counter()-started}),flush=True)
    size=sum(v.size for v in fs.get_file_info(pafs.FileSelector(path,recursive=True)) if v.type==pafs.FileType.File)
    write('chirps_preparation.json',{'status':'READY','dataset':'chirps','seconds_separate':time.perf_counter()-started,
        'zarr':{'stores':[path],'files':len(files),'bytes':size},
        'sedona':{'path':'/user/dataset/chirps','native':True},
        'validation':'All 3652 original daily images preserved; selected 120 dates exact full-array Zarr readback; original SHA256 and identity recorded.'})
    print('FULL_ARCHIVE_PREPARATION_PASS '+json.dumps({'dataset':'chirps','seconds':time.perf_counter()-started,'bytes':size}),flush=True)


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('mode',choices=['setup','ncep','gleam','ecmwf','chirps'])
    args=parser.parse_args()
    if args.mode=='setup':
        setup(); return
    space_guard()
    if args.mode=='ncep':
        prepare_ncep(); return
    client=Client('tcp://192.0.2.64:38786')
    client.wait_for_workers(24,timeout=180)
    info=client.scheduler_info(n_workers=-1)['workers']
    assert len(info)==24 and sum(v['nthreads'] for v in info.values())==24
    assert {v['host'] for v in info.values()}=={'192.0.2.65','192.0.2.66','192.0.2.71','192.0.2.72'}
    try:
        if args.mode=='chirps': prepare_chirps(client)
        else: prepare_hdf(client,args.mode)
    finally:
        client.close()


if __name__=='__main__':
    main()
