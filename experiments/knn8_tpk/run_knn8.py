# 早期 k=8、单次计时 2-3 次的批次入口；正式 k=10 入口见 experiments/knn_tpk/，计时口径 1 次预热 + 5 次正式计时。
import hashlib, json, subprocess, sys, time
from pathlib import Path
import tpk_pipeline as p
from tpk_common import K, MANIFEST_DIR, native_fs

ROOT = Path(__file__).resolve().parent
ORIGINAL = Path('/opt/mgsi-experiments/tpk')
assert K == 8
p.MODES = ['knn']
p.wait_restore = lambda parts, what: None

def ssh(ip, command):
    return subprocess.run(['ssh', '-n', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', 'root@'+ip, command], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True).stdout

def restore_flink(units):
    for ip, names in units.items():
        if names:
            ssh(ip, 'systemctl start ' + ' '.join(names))

def summary():
    out = {}
    for path in ROOT.glob('*_results.jsonl'):
        records = [json.loads(x) for x in path.read_text().splitlines() if x.strip()]
        assert all(x.get('status') == 'PASS' for x in records), path
        if path.name.startswith('zarr_'):
            assert len(records)==1
            numbers=records[0]['measured_runs']
            assert all(len(x['result'])==8 for x in records[0]['runs']), path
        else:
            timed = [x for x in records if str(x.get('run')) != 'warmup']
            numbers = [x['until_final_result_seconds'] for x in timed]
            assert all(len(x['result']) == 8 for x in records), path
        assert len(numbers) in (2,3), (path, len(numbers))
        stat = sum(numbers)/2 if len(numbers)==2 else sorted(numbers)[1]
        key = path.name.replace('_results.jsonl','').replace('sedona_', '')
        out[key] = {'measured': numbers, 'stat': stat, 'rule': 'mean' if len(numbers)==2 else 'median', 'k':8,
                    'records_sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    assert len(out) in (7,8), list(out)
    (ROOT/'knn8_results.json').write_text(json.dumps(out, indent=2))
    print('KNN8_RECORDED_PASS ' + json.dumps({k:v['stat'] for k,v in out.items()}),flush=True)
    return len(out)==8

def main():
    for dataset in p.DATASETS:
        origin = ORIGINAL/(dataset+'_tpk_reference.json')
        reference = json.loads(origin.read_text())
        assert len(reference['knn_cells']) == 10
        reference['knn_cells'] = reference['knn_cells'][:8]
        reference['k'] = 8
        reference['knn8_provenance'] = {'source':str(origin), 'sha256':hashlib.sha256(origin.read_bytes()).hexdigest(),
                                       'selection':'first eight independently validated nearest cells; no reference values recalculated'}
        (ROOT/(dataset+'_tpk_reference.json')).write_text(json.dumps(reference,indent=1))
    info=p.overview()
    assert info['jobs-running']==0 and info['slots-available']==info['slots-total'], info
    units={}
    for ip in ['192.0.2.64']+p.WORKERS:
        lines=ssh(ip,'systemctl list-units --type=service --state=active --plain --no-legend').splitlines()
        units[ip]=[line.split()[0] for line in lines if line.split() and line.split()[0].startswith('mgsi-flink-')]
    (ROOT/'flink_services_before.json').write_text(json.dumps({'overview':info,'units':units},indent=2))
    p.state(status='RUNNING', k=8)
    try:
        p.deploy()
        for ip,names in units.items():
            if names: ssh(ip,'systemctl stop '+' '.join(names))
        p.start_dask()
        for dataset in p.DATASETS:
            if dataset=='ecmwf':
                prep=json.loads((Path(MANIFEST_DIR[dataset])/(dataset+'_preparation.json')).read_text())
                if native_fs().get_file_info(prep['zarr']['stores'][0]).type.name != 'Directory':
                    print('KNN8_PENDING_ARCHIVED_ECMWF_ZARR',flush=True)
                    continue
            p.await_capacity('knn8_zarr_'+dataset)
            p.run_script('run_tpk_dask.py',['--dataset',dataset,'--mode','knn'],'zarr_knn_'+dataset)
        p.stop_dask()
        p.sedona_phase()
        complete=summary()
        p.state(status='QUERIES_COMPLETE' if complete else 'AVAILABLE_QUERIES_COMPLETE', k=8)
    except Exception as error:
        p.state(status='FAILED',error=str(error),k=8)
        raise
    finally:
        for role in ('dask_worker','spark_worker'):
            for ip in p.WORKERS: p._ignore(p.node,ip,'stop',role)
        for role in ('dask_scheduler','spark_master'): p._ignore(p.local_stop,role)
        restore_flink(units)
        deadline=time.monotonic()+180
        while time.monotonic()<deadline:
            current=p.overview_or_none()
            if current and current['taskmanagers']==info['taskmanagers'] and current['slots-total']==info['slots-total']:
                (ROOT/'flink_services_after.json').write_text(json.dumps(current,indent=2))
                saved=json.loads(p.STATE_PATH.read_text()).get('status')
                if saved=='QUERIES_COMPLETE':p.state(status='COMPLETE',k=8)
                if saved=='AVAILABLE_QUERIES_COMPLETE':p.state(status='PARTIAL_PENDING_ECMWF_ZARR',k=8)
                print('KNN8_SERVICES_RESTORED',flush=True)
                break
            time.sleep(2)
        else: raise RuntimeError('Original Flink allocation has not returned')

if __name__=='__main__':main()
