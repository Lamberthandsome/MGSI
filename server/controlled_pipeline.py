"""Serial matched queries, with fail-closed disk/memory monitoring and owned cleanup."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import argparse
import datetime as dt
import fcntl
import json
import os
import shlex
import signal
import subprocess
import time
import urllib.request
from guard import check, ResourceLimit, HOSTS, INTERVAL

ROOT=Path(__file__).resolve().parent
PYTHON='/opt/mgsi-modern/python312/bin/python'
PY37='/opt/mgsi-cluster/python37/bin/python3'
WORKERS=HOSTS[1:]
STATE={'scope':'First GLEAM matched spatial batch','planned_queries':192,'completed':[],
       'user_max_disk_percent':90,'automatic_retry':False,'global_legacy_resume':False}
CURRENT=None

def state(**changes):
    STATE.update(changes); STATE['updated_at']=dt.datetime.now().isoformat()
    p=ROOT/'pipeline_state.json'; t=p.with_suffix('.tmp'); t.write_text(json.dumps(STATE,indent=2)); t.replace(p)
    print('MATCHED_PIPELINE '+json.dumps(STATE),flush=True)

def overview():
    with urllib.request.urlopen('http://192.0.2.64:8081/overview',timeout=10) as stream: return json.load(stream)

def remote(host, command, timeout=150):
    p=subprocess.run(['ssh','-T','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes',
        '-o','ConnectTimeout=8','-o','ServerAliveInterval=5','-o','ServerAliveCountMax=2',
        'root@'+host,command],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=timeout)
    if p.returncode: raise RuntimeError(host+' command failed: '+p.stderr[-1500:]+' '+p.stdout[-1500:])
    return p.stdout

def units(host):
    remote(host,'systemctl start mgsi-flink-taskmanager.service')

def stop_owned_roles():
    for host in HOSTS:
        code=r'''
from pathlib import Path
import json, os, signal, time
prefix=Path(__ROOT__).resolve()
for role in ['dask_scheduler','dask_worker','spark_master','spark_worker']:
    pin=Path('/opt/mgsi-modern/run')/(role+'.json')
    if not pin.exists(): continue
    v=json.loads(pin.read_text()); owner=Path(v.get('owner','/')).resolve()
    if prefix not in owner.parents and owner!=prefix: continue
    proc=Path('/proc')/str(v['pid'])
    if proc.exists():
        s=(proc/'stat').read_text().split()
        assert s[21]==v['start_ticks'], 'PID identity changed; refuse termination'
        os.killpg(v['pid'],signal.SIGTERM)
        deadline=time.monotonic()+30
        while proc.exists() and time.monotonic()<deadline:
            try:
                if (proc/'stat').read_text().split()[2]=='Z': break
            except FileNotFoundError: break
            time.sleep(.2)
        if proc.exists():
            try:
                if (proc/'stat').read_text().split()[2]!='Z': raise RuntimeError('Owned role has not exited')
            except FileNotFoundError: pass
    pin.unlink()
'''.replace('__ROOT__',repr(str(ROOT)))
        if host==HOSTS[0]:
            p=subprocess.run([PYTHON,'-B','-'],input=code,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=50)
            if p.returncode: raise RuntimeError(p.stderr[-1500:])
        else: remote(host,shlex.quote(PYTHON)+' -B -c '+shlex.quote(code),timeout=55)

def cancel_owned_flink():
    p=ROOT/'gleam_mgsi/state.json'
    if not p.exists(): return
    v=json.loads(p.read_text()); job=v.get('job_id')
    if not job: return
    with urllib.request.urlopen('http://192.0.2.64:8081/jobs/'+job,timeout=10) as stream: current=json.load(stream)
    if current['state'] not in ('FINISHED','FAILED','CANCELED'):
        subprocess.run(['/opt/mgsi-cluster/flink/bin/flink','cancel','-m','192.0.2.64:8081',job],check=True,timeout=45)

def restore():
    errors=[]
    try: stop_owned_roles()
    except Exception as exc: errors.append('Owned role cleanup: '+str(exc))
    try:
        subprocess.run(['systemctl','start','mgsi-flink-jobmanager'],check=True,timeout=120)
        try: cancel_owned_flink()
        except Exception as exc: errors.append('Owned query cleanup: '+str(exc))
        with ThreadPoolExecutor(max_workers=4) as pool: list(pool.map(units,WORKERS))
        until=time.monotonic()+180
        while time.monotonic()<until:
            v=overview()
            if v['taskmanagers']==24 and v['slots-available']==v['slots-total']==72 and not v['jobs-running']:
                (ROOT/'environment_final.json').write_text(json.dumps(v,indent=2)); break
            time.sleep(1)
        else: errors.append('Idle 24/72 profile not confirmed: '+str(v))
    except Exception as exc: errors.append('Environment restore: '+str(exc))
    state(environment_restored=not errors,cleanup_errors=errors)

def deploy_native_metadata():
    source=ROOT/'gleam_mgsi'
    files=[source/'physical_index.with_blocks.json']
    for host in WORKERS:
        remote(host,'mkdir -p '+shlex.quote(str(source)))
        subprocess.run(['scp','-q','-l','32768',*[str(p) for p in files],
            'root@'+host+':'+str(source)+'/'],check=True,timeout=120)

def terminate(proc):
    if proc.poll() is None:
        os.killpg(proc.pid,signal.SIGTERM)
        try: proc.wait(timeout=90)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid,signal.SIGKILL); proc.wait(timeout=15)

def child(label,argv,timeout=14400):
    global CURRENT
    check(running=False)
    state(status='RUNNING',current=label)
    with (ROOT/(label+'.log')).open('x') as stream:
        proc=subprocess.Popen(argv,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True)
    CURRENT=proc; start=time.monotonic()
    state(current_child_pid=proc.pid)
    try:
        while proc.poll() is None:
            check(running=True)
            if time.monotonic()-start>timeout: raise RuntimeError('Stage deadline exceeded: '+label)
            time.sleep(INTERVAL)
        check(running=True)
        if proc.returncode: raise RuntimeError('{} failed rc={} (see its preserved log)'.format(label,proc.returncode))
    except BaseException:
        terminate(proc); raise
    finally: CURRENT=None
    STATE['completed'].append(label); state(last_completed=label,current_child_pid=None)

def main():
    assert not (ROOT/'pipeline_state.json').exists(), 'Fresh run directory required.'
    authorization=json.loads((ROOT/'authorization.json').read_text())
    assert authorization['matched_baselines_authorized'] and authorization['disk_max_percent']==90
    lock=(ROOT/'run.lock').open('a+'); fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    def interrupted(signum,frame): raise InterruptedError('Controller interrupted')
    signal.signal(signal.SIGTERM,interrupted); signal.signal(signal.SIGINT,interrupted)
    try:
        check(running=False)
        initial=overview(); assert initial['jobs-running']==0 and initial['slots-available']==initial['slots-total']==72,initial
        (ROOT/'environment_initial.json').write_text(json.dumps(initial,indent=2))
        state(status='PREPARING',current='prepare_current')
        child('prepare_current',[PYTHON,str(ROOT/'prepare_current.py')],timeout=3600)
        child('deploy_native_metadata',[PYTHON,str(__file__),'--deploy-native'],timeout=600)
        child('gleam_mgsi',[PY37,str(ROOT/'gleam_mgsi/run_resident.py')],timeout=7200)
        child('verify_mgsi',[PYTHON,str(ROOT/'gleam_mgsi/summarize.py'),'--stage-only'],timeout=120)
        for label,stage,extra in [('gleam_native','native',[]),('gleam_zarr','dask',['--dask-methods','xarray_zarr']),('gleam_sedona','sedona',[])]:
            child(label,[PYTHON,str(ROOT/label/'run_stage.py'),'--stage',stage,'--datasets','gleam',
                '--regions',*[str(i) for i in range(1,9)],'--repeats','6',*extra])
        child('final_verification',[PYTHON,str(ROOT/'summarize.py')],timeout=180)
        state(status='COMPLETE',current=None,completed_queries=192)
    except ResourceLimit as exc:
        state(status='STOPPED_RESOURCE_LIMIT',error=str(exc),current_child_pid=None)
    except BaseException as exc:
        state(status='FAILED',error=str(exc),current_child_pid=None)
    finally:
        restore()
        try: check(running=True)
        except Exception as exc: state(final_resource_notice=str(exc))
        fcntl.flock(lock,fcntl.LOCK_UN); lock.close()

if __name__=='__main__':
    parser=argparse.ArgumentParser(); parser.add_argument('--deploy-native',action='store_true'); args=parser.parse_args()
    if args.deploy_native: deploy_native_metadata()
    else: main()
