"""Preserve each SQL result and restore Flink after switching engines."""
import hashlib
import json
from pathlib import Path
import subprocess
import time
import urllib.request

ROOT=Path(__file__).resolve().parent
WORKERS=['192.0.2.65','192.0.2.66','192.0.2.71','192.0.2.72']
STATE={'completed':[]}


def state(**values):
    STATE.update(values)
    (ROOT/'sql_queue_state.json').write_text(json.dumps(STATE,indent=2))


def get(path='/overview'):
    with urllib.request.urlopen('http://192.0.2.64:8081'+path,timeout=20) as stream:
        return json.load(stream)


def idle():
    deadline=time.time()+120
    while time.time()<deadline:
        try:
            r=get()
            if r['taskmanagers']==24 and r['slots-available']==72 and r['jobs-running']==0:
                return r
        except Exception:
            pass
        time.sleep(1)
    raise RuntimeError('Flink did not become idle')


def worker(ip,command):
    subprocess.run(['ssh','-n','-o','BatchMode=yes','-o','ConnectTimeout=10','root@'+ip,command],check=True)


def spark_mode():
    idle()
    for ip in WORKERS:
        worker(ip,'systemctl stop mgsi-flink-taskmanager.service '+' '.join('mgsi-flink-taskmanager@{}.service'.format(i) for i in range(1,7)))
    subprocess.run(['systemctl','stop','mgsi-flink-jobmanager'],check=True)
    subprocess.run(['systemctl','start','mgsi-spark-master'],check=True)
    for ip in WORKERS:
        worker(ip,'systemctl start mgsi-spark-worker')
    Path('/opt/mgsi-cluster/state/active-engine').write_text('spark21\n')


def restore():
    for ip in WORKERS:
        worker(ip,'systemctl stop mgsi-spark-worker')
    subprocess.run(['systemctl','stop','mgsi-spark-master'],check=True)
    subprocess.run(['systemctl','start','mgsi-flink-jobmanager'],check=True)
    for ip in WORKERS:
        worker(ip,'systemctl start mgsi-flink-taskmanager.service')
    Path('/opt/mgsi-cluster/state/active-engine').write_text('flink\n')
    (ROOT/'environment_after_sql.json').write_text(json.dumps(idle(),indent=2))


def main():
    deadline=time.time()+600
    while not (ROOT/'sql_input_manifest.json').exists() and time.time()<deadline:
        time.sleep(2)
    assert (ROOT/'sql_input_manifest.json').exists(), 'Input preparation did not finish.'
    native=json.loads((ROOT/'state.json').read_text())
    assert native['status']=='COMPLETE' and native['job_state']=='FINISHED'
    assert json.loads((ROOT/'sql_input_manifest.json').read_text())['status']=='PASS'
    assert not (ROOT/'sql_results.jsonl').exists()
    pin={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in ('run_tenyear_sql_queue.py','run_tenyear_flink_sql.py','run_tenyear_spark_sql.py','sql_input_manifest.json','golden_results.json')}
    (ROOT/'sql_pin.json').write_text(json.dumps(pin,indent=2))
    for region in range(1,9):
        for repeat in ((1,2) if region in (1,8) else (1,)):
            idle()
            state(status='FLINK_SQL',region=region,repeat=repeat)
            command=['/opt/mgsi-cluster/flink/bin/flink','run','-m','192.0.2.64:8081','-p','72','-C','file:///opt/mgsi-experiments/cross_baseline_audit/flink-sql-parquet_2.11-1.11.3.jar','-py',str(ROOT/'run_tenyear_flink_sql.py'),'--region',str(region),'--repeat',str(repeat)]
            with (ROOT/'flink_sql_R{}_{}.log'.format(region,repeat)).open('wb') as stream:
                subprocess.run(command,cwd=str(ROOT),stdout=stream,stderr=subprocess.STDOUT,check=True)
            rows=[json.loads(line) for line in (ROOT/'sql_results.jsonl').read_text().splitlines()]
            assert rows[-1]['status']=='PASS'
            STATE['completed'].append({k:rows[-1][k] for k in ('method','region','repeat','job_id','until_final_120_month_results_seconds')})
            state(status='CASE_COMPLETE')
    try:
        state(status='SWITCHING_TO_SPARK')
        spark_mode()
        state(status='SPARK_SQL')
        with (ROOT/'spark_tenyear_sql.log').open('wb') as stream:
            process=subprocess.Popen(['/opt/mgsi-cluster/spark21/bin/spark-submit','--master','spark://192.0.2.64:7077','--total-executor-cores','72','--executor-cores','3','--executor-memory','8g',str(ROOT/'run_tenyear_spark_sql.py')],cwd=str(ROOT),stdout=stream,stderr=subprocess.STDOUT)
            with (ROOT/'spark_cluster_samples.jsonl').open('a') as samples:
                while process.poll() is None:
                    snapshot={'time':time.time()}
                    for key,url in [('master','http://192.0.2.64:8080/json/'),('applications','http://192.0.2.64:4040/api/v1/applications')]:
                        try:
                            with urllib.request.urlopen(url,timeout=5) as reply:
                                snapshot[key]=json.load(reply)
                        except Exception as exc:
                            snapshot[key+'_error']=str(exc)
                    samples.write(json.dumps(snapshot)+'\n')
                    samples.flush()
                    time.sleep(3)
            assert process.returncode==0,process.returncode
    finally:
        state(status='RESTORING_FLINK')
        restore()
    rows=[json.loads(line) for line in (ROOT/'sql_results.jsonl').read_text().splitlines()]
    assert len(rows)==26 and all(row['status']=='PASS' for row in rows)
    state(status='COMPLETE',science_queries_passed=len(rows))
    print('TENYEAR_SQL_COMPLETE '+json.dumps(STATE),flush=True)


if __name__=='__main__':
    main()
