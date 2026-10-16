"""Exclusive stage controller; restores the idle legacy cluster on success or failure."""
from concurrent.futures import ThreadPoolExecutor
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import urllib.request

ROOT=Path(__file__).resolve().parent
BASE=Path('/opt/mgsi-modern')
PYTHON=str(BASE/'python312/bin/python')
WORKERS=['192.0.2.65','192.0.2.66','192.0.2.71','192.0.2.72']
ALLOWED_WORKERS=tuple(WORKERS)
STATE={'completed':[]}


def state(**values):
    STATE.update(values)
    STATE['updated_at']=dt.datetime.now().isoformat()
    target=ROOT/'stage_state.json'
    temporary=target.with_suffix('.tmp')
    temporary.write_text(json.dumps(STATE,indent=2))
    temporary.replace(target)
    print('STAGE_STATE '+json.dumps(STATE),flush=True)


def overview():
    with urllib.request.urlopen('http://192.0.2.64:8081/overview',timeout=10) as stream: return json.load(stream)


def remote(ip,command):
    subprocess.run(['ssh','-n','-o','BatchMode=yes','-o','ConnectTimeout=10','root@'+ip,command],check=True)


def node(ip,action,role,threads=3):
    remote(ip,'source /opt/mgsi-cluster/env.sh; /opt/mgsi-cluster/python37/bin/python3 '+
        shlex.quote(str(ROOT/'node_service.py'))+' '+action+' '+role+' --nthreads '+str(threads))


def local(action,role,threads=3):
    subprocess.run(['/opt/mgsi-cluster/python37/bin/python3',str(ROOT/'node_service.py'),action,role,'--nthreads',str(threads)],check=True)


def deploy(ip):
    remote(ip,'mkdir -p '+shlex.quote(str(ROOT)))
    names=['node_service.py','matrix_common.py','modern_common.py','prepare_matrix.py']
    subprocess.run(['scp',*[str(ROOT/name) for name in names],'root@'+ip+':'+str(ROOT)+'/'],check=True)
    remote(ip,shlex.quote(PYTHON)+' -c '+shlex.quote('import ssl,xarray,distributed,zarr,h5py,rasterio,pyarrow,scipy; print("SDK_IMPORTS_PASS")'))


def stop_roles():
    errors=[]
    for role in ('dask_worker','spark_worker'):
        with ThreadPoolExecutor(max_workers=4) as pool:
            results=list(pool.map(lambda ip: stop_one(ip,role),WORKERS))
        errors.extend(error for error in results if error)
    for role in ('dask_scheduler','spark_master'):
        try: local('stop',role)
        except Exception as error: errors.append(str(error))
    return errors


def stop_one(ip,role):
    try: node(ip,'stop',role)
    except Exception as error: return str(error)
    return None


def restore():
    errors=stop_roles()
    subprocess.run(['systemctl','start','mgsi-flink-jobmanager'],check=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda ip: remote(ip,'systemctl start mgsi-flink-taskmanager.service'),WORKERS))
    deadline=time.monotonic()+180
    value=None
    while time.monotonic()<deadline:
        try: value=overview()
        except OSError:
            time.sleep(1); continue
        minimum=len(WORKERS)*4
        if value['taskmanagers']>=minimum and value['slots-available']==value['slots-total'] and value['jobs-running']==0:
            (ROOT/'environment_final.json').write_text(json.dumps(value,indent=2))
            if errors: raise RuntimeError(errors)
            state(environment_restored=True)
            return
        time.sleep(1)
    raise RuntimeError('Legacy restore failed: '+str(value))


def run_script(script,args,label,env):
    state(status='RUNNING',current=label)
    with (ROOT/(label+'.log')).open('x') as stream:
        subprocess.run([PYTHON,str(ROOT/script),*args],env=env,stdout=stream,stderr=subprocess.STDOUT,check=True)
    STATE['completed'].append(label)
    state(status='RUNNING',last_completed=label)


def main():
    global WORKERS
    parser=argparse.ArgumentParser()
    parser.add_argument('--stage',choices=['native','prepare','dask','sedona'],required=True)
    parser.add_argument('--datasets',nargs='+',choices=['ncep','chirps','gleam','ecmwf'],required=True)
    parser.add_argument('--regions',nargs='+',type=int,default=list(range(1,9)))
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--worker-hosts',nargs='+',choices=ALLOWED_WORKERS,default=list(ALLOWED_WORKERS))
    parser.add_argument('--dask-methods',nargs='+',choices=['xarray_native','xarray_zarr'],default=['xarray_native','xarray_zarr'])
    args=parser.parse_args()
    WORKERS=args.worker_hosts
    assert len(set(WORKERS))==len(WORKERS) and len(WORKERS) in (2,4)
    assert args.stage!='prepare' or len(WORKERS)==4,'Full preparation retains its four-worker profile.'
    processes=len(WORKERS)*4
    cores=processes*3
    assert not (ROOT/'stage_state.json').exists(),'Fresh stage directory required.'
    prior=overview()
    assert prior['taskmanagers']>=processes and prior['slots-available']==prior['slots-total'] and prior['jobs-running']==0,prior
    (ROOT/'environment_initial.json').write_text(json.dumps(prior,indent=2))
    profile={'worker_hosts':WORKERS,'worker_nodes':len(WORKERS),'processes_or_executors':processes,
             'threads_or_cores':cores,'memory_per_process_gib':8,
             'purpose':'matched_four_worker_batch' if len(WORKERS)==4 else 'two_worker_functional_pilot',
             'formal_four_worker_result':True}
    (ROOT/'execution_profile.json').write_text(json.dumps(profile,indent=2))
    protocol=json.loads((ROOT/'protocol.json').read_text())
    protocol.update(execution_profile=profile,query_compute='{} workers; {} processes/executors x 3 threads/cores; {} total.'.format(len(WORKERS),processes,cores))
    if len(WORKERS)==2: protocol['scope_limit']='Two-worker functionality and pilot timing only; separate from four-worker MGSI comparison.'
    (ROOT/'protocol.json').write_text(json.dumps(protocol,indent=2))
    names=[p for p in ROOT.iterdir() if p.suffix in ('.py','.java','.json') or p.name=='modern_freeze.txt']
    (ROOT/'stage_pin.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in names},indent=2))
    state(status='DEPLOYING',stage=args.stage,datasets=args.datasets,regions=args.regions,repeats=args.repeats,execution_profile=profile)
    with ThreadPoolExecutor(max_workers=4) as pool: list(pool.map(deploy,WORKERS))
    env=os.environ.copy()
    env.update(LIBHDFS_OPTS='-Xmx1g -XX:ActiveProcessorCount=3',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1')
    threads=1 if args.stage=='prepare' else 3
    try:
        state(status='STOPPING_IDLE_FLINK')
        current=overview()
        assert current['taskmanagers']>=processes and current['slots-available']==current['slots-total'] and current['jobs-running']==0,current
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda ip:remote(ip,'systemctl stop mgsi-flink-taskmanager.service '+
                ' '.join('mgsi-flink-taskmanager@{}.service'.format(i) for i in range(1,7))),WORKERS))
        subprocess.run(['systemctl','stop','mgsi-flink-jobmanager'],check=True)
        if args.stage in ('native','prepare','dask'):
            local('start','dask_scheduler',threads)
            with ThreadPoolExecutor(max_workers=4) as pool: list(pool.map(lambda ip:node(ip,'start','dask_worker',threads),WORKERS))
            for dataset in args.datasets:
                if args.stage=='prepare':
                    run_script('prepare_matrix.py',[dataset],'prepare_'+dataset,env)
                else:
                    methods=['xarray_native'] if args.stage=='native' else args.dask_methods
                    for method in methods:
                        run_script('run_matrix_dask.py',['--dataset',dataset,'--method',method,'--regions',
                            *[str(v) for v in args.regions],'--repeats',str(args.repeats),'--worker-hosts',*WORKERS],method+'_'+dataset,env)
        else:
            local('start','spark_master')
            with ThreadPoolExecutor(max_workers=4) as pool: list(pool.map(lambda ip:node(ip,'start','spark_worker'),WORKERS))
            classes=ROOT/'classes'; classes.mkdir()
            subprocess.run([str(BASE/'jdk17/bin/javac'),'-cp',str(BASE/'spark356/jars/*')+':'+str(BASE/'sedona-jars/*'),
                '-d',str(classes),str(ROOT/'SedonaMatrix.java')],check=True)
            subprocess.run([str(BASE/'jdk17/bin/jar'),'cf',str(ROOT/'sedona-matrix.jar'),'-C',str(classes),'.'],check=True)
            env.update(JAVA_HOME=str(BASE/'jdk17'),SPARK_HOME=str(BASE/'spark356'),SPARK_LOCAL_IP='192.0.2.64')
            env.pop('SPARK_DIST_CLASSPATH',None)
            jars=','.join(str(p) for p in (BASE/'sedona-jars').glob('*.jar'))
            command=[str(BASE/'spark356/bin/spark-submit'),'--master','spark://192.0.2.64:7078','--class','SedonaMatrix',
                '--jars',jars,'--executor-cores','3','--executor-memory','8g','--total-executor-cores',str(cores),'--driver-memory','4g',
                '--conf','spark.driver.host=192.0.2.64','--conf','spark.driver.bindAddress=192.0.2.64',
                '--conf','spark.cores.max='+str(cores),'--conf','spark.executor.instances='+str(processes),str(ROOT/'sedona-matrix.jar'),str(ROOT),
                ','.join(args.datasets),','.join(str(r) for r in args.regions),str(args.repeats),','.join(WORKERS)]
            state(status='RUNNING',current='sedona_matrix')
            with (ROOT/'sedona_matrix.log').open('x') as stream:
                subprocess.run(command,env=env,stdout=stream,stderr=subprocess.STDOUT,check=True)
            STATE['completed'].append('sedona_matrix')
        run_script('summarize.py',['--stage-only'],'verify_stage',env)
        state(status='COMPLETE')
    except Exception as error:
        state(status='FAILED',error=str(error)); raise
    finally:
        restore()


if __name__=='__main__':
    import signal
    def interrupted(signum,frame): raise InterruptedError('Resource supervisor stopped this stage')
    signal.signal(signal.SIGTERM,interrupted)
    main()
