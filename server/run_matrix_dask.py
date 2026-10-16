"""Compute complete monthly results and retain trace evidence before publishing timings."""
import argparse
import gzip
import hashlib
import json
from pathlib import Path
import time

import dask
from distributed import Client, get_task_stream
import numpy as np
import xarray as xr

from matrix_common import mapper, merge, hdf_native, chirps_native, decoded, probe
from modern_common import ncep_native
from prepare_matrix import validate

ROOT=Path(__file__).resolve().parent
HOSTS={'192.0.2.65','192.0.2.66','192.0.2.71','192.0.2.72'}


def json_default(value):
    if isinstance(value,bytes): return {'bytes_hex':value.hex()}
    if isinstance(value,np.generic): return value.item()
    raise TypeError(type(value).__name__)


def zarr_query(dataset,preparation,box):
    graphs, labels, opened=[],[],[]
    try:
        for path in preparation['zarr']['stores']:
            ds=xr.open_zarr(mapper(path),consolidated=True,decode_times=False,mask_and_scale=False)
            opened.append(ds)
            # The earlier complete NCEP store encodes CF datetimes as integers;
            # its explicit month coordinate retains the original grouping.
            times=(ds.month.values if dataset=='ncep' else ds.time.values).astype(str)
            indices=np.arange(len(times)) if dataset=='ncep' else np.flatnonzero(np.asarray([label[8:10]=='15' for label in times]))
            lat,lon=ds.lat.values,ds.lon.values
            ri=np.flatnonzero((lat>=box['lat_min']) & (lat<=box['lat_max']))
            ci=np.flatnonzero((lon>=box['lon_min']) & (lon<=box['lon_max']))
            value=decoded(ds.value.isel(time=indices,lat=ri,lon=ci))
            graphs.extend([value.sum(dim=('lat','lon'),skipna=True,dtype=np.float64),value.count(dim=('lat','lon'))])
            labels.append(times[indices])
        results=dask.compute(*graphs)
        rows=[]
        for index,times in enumerate(labels):
            monthly={}
            for label,total,count in zip(times,results[index*2].values,results[index*2+1].values):
                target=monthly.setdefault(label[:7],{'sum':0.0,'count':0})
                target['sum']+=float(total); target['count']+=int(count)
            rows.append({'monthly':monthly})
        return merge(rows)
    finally:
        for ds in opened: ds.close()


def native_query(dataset,source,box,nonce):
    tasks=[]
    for file in source['files']:
        if dataset=='ncep':
            tasks.append(dask.delayed(ncep_native,pure=False)(file,box,nonce))
        elif dataset=='chirps':
            tasks.append(dask.delayed(chirps_native,pure=False)(file,box,nonce))
        else:
            chunks=file['physical_chunks']
            height,width=(chunks[1]*2,chunks[2]*4) if dataset=='ecmwf' else (chunks[1]*4,chunks[2]*12)
            lat,lon=np.asarray(file['lat']),np.asarray(file['lon'])
            for y0 in range(0,file['shape'][1],height):
                for x0 in range(0,file['shape'][2],width):
                    y1,x1=min(y0+height,len(lat)),min(x0+width,len(lon))
                    # Coordinate metadata only; array data is read inside timed worker tasks.
                    if not np.any((lat[y0:y1]>=box['lat_min']) & (lat[y0:y1]<=box['lat_max'])): continue
                    if not np.any((lon[x0:x1]>=box['lon_min']) & (lon[x0:x1]<=box['lon_max'])): continue
                    tasks.append(dask.delayed(hdf_native,pure=False)(file,(y0,y1,x0,x1),box,nonce))
    rows=list(dask.compute(*tasks))
    return merge(rows),rows


def main():
    global HOSTS
    parser=argparse.ArgumentParser()
    parser.add_argument('--dataset',choices=['ncep','chirps','gleam','ecmwf'],required=True)
    parser.add_argument('--method',choices=['xarray_native','xarray_zarr'],required=True)
    parser.add_argument('--regions',nargs='+',type=int,default=list(range(1,9)))
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--worker-hosts',nargs='+',choices=sorted(HOSTS),default=sorted(HOSTS))
    args=parser.parse_args()
    HOSTS=set(args.worker_hosts)
    assert len(HOSTS)==len(args.worker_hosts) and len(HOSTS) in (2,4)
    process_count=len(HOSTS)*4
    thread_count=process_count*3
    name=args.method+'_'+args.dataset
    output=ROOT/(name+'_results.jsonl')
    assert not output.exists(),'Use a fresh directory for retries; preserve passed records.'
    inputs=json.loads((ROOT/'matrix_inputs.json').read_text())
    source=inputs[args.dataset]
    boxes=json.loads((ROOT/'boxes.json').read_text())
    reference=json.loads((ROOT/(args.dataset+'_reference.json')).read_text())
    preparation=json.loads((ROOT/(args.dataset+'_preparation.json')).read_text()) if args.method=='xarray_zarr' else None
    initialization=time.perf_counter()
    client=Client('tcp://192.0.2.64:38786')
    client.wait_for_workers(process_count,timeout=180)
    workers=client.scheduler_info(n_workers=-1)['workers']
    assert len(workers)==process_count and sum(v['nthreads'] for v in workers.values())==thread_count
    assert {v['host'] for v in workers.values()}==HOSTS
    probes=client.run(probe)
    initialization=time.perf_counter()-initialization
    worker_config={address:{k:v.get(k) for k in ('host','name','nthreads','memory_limit','status')} for address,v in workers.items()}
    (ROOT/(name+'_initialization.json')).write_text(json.dumps({'workers':worker_config,'probe':probes,'seconds_separate':initialization},indent=2))
    names=['matrix_common.py','modern_common.py','prepare_matrix.py','run_matrix_dask.py','matrix_inputs.json','boxes.json',args.dataset+'_reference.json','protocol.json']
    if preparation: names.append(args.dataset+'_preparation.json')
    (ROOT/(name+'_pin.json')).write_text(json.dumps({n:hashlib.sha256((ROOT/n).read_bytes()).hexdigest() for n in names},indent=2))
    try:
        for region in args.regions:
            for repeat in range(1,args.repeats+1):
                nonce=name+'_R{}_{}'.format(region,repeat)
                with get_task_stream(client=client) as trace:
                    timer=time.perf_counter()
                    if args.method=='xarray_native':
                        monthly,rows=native_query(args.dataset,source,boxes[str(region)],nonce)
                    else:
                        monthly=zarr_query(args.dataset,preparation,boxes[str(region)])
                        rows=[]
                    elapsed=time.perf_counter()-timer
                validate(monthly,reference[str(region)])
                active={row['host'] for row in rows if row['monthly']} if rows else {workers[t['worker']]['host'] for t in trace.data if t['worker'] in workers}
                assert active==HOSTS,(nonce,active)
                trace_path=ROOT/(nonce+'_tasks.json.gz')
                with gzip.open(trace_path,'wt') as stream: json.dump(trace.data,stream,default=json_default)
                record={'status':'PASS','method':args.method,'dataset':args.dataset,'region':region,'repeat':repeat,
                    'mode':'warmup' if repeat==1 else 'measured','measured_repeat':repeat-1,
                    'until_final_120_month_results_seconds':elapsed,'monthly_values':monthly,'active_data_hosts':sorted(active),
                    'workers':process_count,'threads':thread_count,'declared_worker_hosts':sorted(HOSTS),
                    'cluster_profile':'matched_four_worker_batch' if len(HOSTS)==4 else 'two_worker_functional_pilot',
                    'task_events':len(trace.data),'task_trace_file':trace_path.name,
                    'task_trace_sha256':hashlib.sha256(trace_path.read_bytes()).hexdigest(),'worker_rows':rows,
                    'worker_read_bytes':sum(row['read_bytes'] for row in rows) if rows else None,
                    'initialization_seconds_separate':initialization,'scope':source['scope'],
                    'protocol':'Resident Dask; no persist/result cache; initialization and conversion separate; uncontrolled OS/HDFS caches; full monthly result return included.'}
                with output.open('a') as stream: stream.write(json.dumps(record,allow_nan=False,default=json_default)+'\n')
                print('MATRIX_DASK_QUERY_PASS '+json.dumps({k:record[k] for k in ('method','dataset','region','repeat','until_final_120_month_results_seconds','active_data_hosts','task_events','worker_read_bytes')}),flush=True)
        (ROOT/(name+'_state.json')).write_text(json.dumps({'status':'COMPLETE','queries':len(args.regions)*args.repeats}))
    finally:
        client.close()


if __name__=='__main__':
    main()
