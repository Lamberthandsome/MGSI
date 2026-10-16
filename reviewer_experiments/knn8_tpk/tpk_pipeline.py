"""Orchestrate the 2026-10-13 temporal/point/kNN new-baseline batch.

Stage order: deploy -> stop idle Flink -> Dask up -> references + Zarr queries
per dataset -> Dask down -> compile + Spark up -> Sedona queries per dataset ->
services restored -> summary. Every stage is resumable: completed units are
recorded in pipeline_state.json and query scripts refuse to overwrite passed
results, so an interrupted pipeline is simply rerun.

Restores of the relocated GLEAM and ERA5-Sedona stores run outside this
pipeline (separate restore_store.py invocations); the pipeline waits for their
per-part state files to reach COMPLETE before using a dataset.
"""
import json
import os
import shlex
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).resolve().parent
BASE = Path('/opt/mgsi-modern')
PYTHON = '/opt/mgsi-modern/python312/bin/python'
CLUSTER_PYTHON = '/opt/mgsi-cluster/python37/bin/python3'
WORKERS = ['192.0.2.65', '192.0.2.66', '192.0.2.71', '192.0.2.72']
REST = 'http://192.0.2.64:8081'
DATASETS = ['ncep', 'merra2', 'ecmwf', 'gleam']
MODES = ['temporal_1yr', 'temporal_10yr', 'point', 'knn']
STATE_PATH = ROOT / 'pipeline_state.json'
RESTORE_STATES = ROOT / 'restore_states'

sys.path.insert(0, str(ROOT))
from restore_store import await_capacity, disk_percent
from tpk_common import MANIFEST_DIR


def state(**values):
    current = json.loads(STATE_PATH.read_text()) if STATE_PATH.exists() else {}
    current.update(values, updated_at=time.strftime('%Y-%m-%dT%H:%M:%S'))
    STATE_PATH.write_text(json.dumps(current, indent=1))
    return current


def completed():
    return set(json.loads(STATE_PATH.read_text()).get('completed', [])) if STATE_PATH.exists() else set()


def mark(label):
    current = state()
    done = set(current.get('completed', []))
    done.add(label)
    state(completed=sorted(done), last_completed=label)
    print('STAGE_COMPLETE ' + label, flush=True)


def overview():
    with urllib.request.urlopen(REST + '/overview', timeout=10) as stream:
        return json.load(stream)


def remote(ip, command):
    subprocess.run(['ssh', '-n', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', 'root@' + ip, command], check=True)


def node(ip, action, role):
    remote(ip, 'source /opt/mgsi-cluster/env.sh; ' + CLUSTER_PYTHON + ' ' +
           shlex.quote(str(ROOT / 'node_service.py')) + ' ' + action + ' ' + role)


def deploy():
    if 'deploy' in completed():
        return
    for ip in WORKERS:
        remote(ip, 'mkdir -p ' + shlex.quote(str(ROOT)))
        subprocess.run(['scp', str(ROOT / 'node_service.py'), str(ROOT / 'tpk_common.py'), 'root@' + ip + ':' + str(ROOT) + '/'], check=True)
        remote(ip, shlex.quote(PYTHON) + ' -c ' + shlex.quote('import xarray,distributed,zarr,h5py,pyarrow; print("SDK_IMPORTS_PASS")'))
    mark('deploy')


def dask_alive():
    try:
        with socket.create_connection(('192.0.2.64', 38786), timeout=3):
            return True
    except OSError:
        return False


def flink_running():
    try:
        with urllib.request.urlopen(REST + '/overview', timeout=5) as stream:
            return json.load(stream)['taskmanagers'] > 0
    except OSError:
        return False


def stop_flink():
    if flink_running():
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda ip: remote(ip, 'systemctl stop mgsi-flink-taskmanager.service ' +
                ' '.join('mgsi-flink-taskmanager@{}.service'.format(i) for i in range(1, 7))), WORKERS))
        subprocess.run(['systemctl', 'stop', 'mgsi-flink-jobmanager'], check=True)
        deadline = time.monotonic() + 120
        while flink_running() and time.monotonic() < deadline:
            time.sleep(2)
        assert not flink_running(), 'Flink did not stop'
    if 'stop_flink' not in completed():
        prior = overview_or_none()
        if prior:
            (ROOT / 'environment_initial.json').write_text(json.dumps(prior, indent=1))
        mark('stop_flink')


def restore_services():
    for role in ('dask_worker', 'spark_worker'):
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda ip: _ignore(node, ip, 'stop', role), WORKERS))
    for role in ('dask_scheduler', 'spark_master'):
        _ignore(local_stop, role)
    subprocess.run(['systemctl', 'start', 'mgsi-flink-jobmanager'], check=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda ip: remote(ip, 'systemctl start mgsi-flink-taskmanager.service'), WORKERS))
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        try:
            value = overview()
        except OSError:
            time.sleep(2)
            continue
        if value['taskmanagers'] >= 16 and value['slots-available'] == value['slots-total'] and value['jobs-running'] == 0:
            (ROOT / 'environment_final.json').write_text(json.dumps(value, indent=1))
            mark('services_restored')
            return
        time.sleep(2)
    raise RuntimeError('Flink restore failed: ' + str(value))


def overview_or_none():
    try:
        with urllib.request.urlopen(REST + '/overview', timeout=10) as stream:
            return json.load(stream)
    except OSError:
        return None


def start_dask():
    if dask_alive():
        mark('dask_up')
        return
    # Clear any stale roles/pins from a previous interrupted run first.
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda ip: _ignore(node, ip, 'stop', 'dask_worker'), WORKERS))
    _ignore(local_stop, 'dask_scheduler')
    await_capacity('start_dask')
    subprocess.run([CLUSTER_PYTHON, str(ROOT / 'node_service.py'), 'start', 'dask_scheduler'], check=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda ip: node(ip, 'start', 'dask_worker'), WORKERS))
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        if dask_alive():
            break
        time.sleep(2)
    else:
        raise RuntimeError('Dask scheduler did not come up')
    time.sleep(15)
    mark('dask_up')


def _ignore(function, *arguments):
    try:
        function(*arguments)
    except Exception as error:
        print('IGNORED ' + repr(error)[:200], flush=True)


def local_stop(role):
    subprocess.run([CLUSTER_PYTHON, str(ROOT / 'node_service.py'), 'stop', role], check=True)


def stop_dask():
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda ip: _ignore(node, ip, 'stop', 'dask_worker'), WORKERS))
    _ignore(local_stop, 'dask_scheduler')
    mark('dask_down')


def run_script(script, arguments, label):
    if label in completed():
        return
    state(status='RUNNING', current=label)
    env = os.environ.copy()
    env.update(PYTHONPATH=str(ROOT), LIBHDFS_OPTS='-Xmx1g -XX:ActiveProcessorCount=3',
               OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1')
    log = ROOT / (label + '.log')
    with log.open('a') as stream:
        stream.write('=== RUN {} {} ===\n'.format(time.strftime('%Y-%m-%dT%H:%M:%S'), label))
        stream.flush()
        result = subprocess.run([PYTHON, str(ROOT / script), *arguments], env=env, cwd=str(ROOT),
                                stdout=stream, stderr=subprocess.STDOUT)
        stream.flush()
    if result.returncode:
        tail = log.read_text(errors='replace')[-2500:]
        raise RuntimeError(label + ' failed rc=' + str(result.returncode) + '; log tail:\n' + tail)
    mark(label)


def restore_ready(parts):
    for part in parts:
        path = RESTORE_STATES / (part + '.json')
        if not path.exists() or json.loads(path.read_text()).get('status') != 'COMPLETE':
            return False
    return True


def wait_restore(parts, what):
    while not restore_ready(parts):
        disks = disk_percent()
        print('WAIT_RESTORE ' + json.dumps({'for': what, 'missing': [p for p in parts if not restore_ready([p])],
                                            'disks': disks}), flush=True)
        time.sleep(300)


GLEAM_PARTS = ['gleam_zarr_{}'.format(y) for y in range(1981, 1991)] + ['gleam_sedona_{}'.format(y) for y in range(1981, 1991)]
ECMWF_SEDONA_PARTS = ['ecmwf_sedona_{}'.format(y) for y in range(1980, 1990)]
RESTORE_WAIT = {'gleam': GLEAM_PARTS, 'ecmwf_sedona': ECMWF_SEDONA_PARTS}


def zarr_phase():
    start_dask()
    for dataset in DATASETS:
        if dataset == 'gleam':
            wait_restore(RESTORE_WAIT['gleam'], 'gleam stores')
        run_script('prepare_tpk_reference.py', ['--dataset', dataset], 'reference_' + dataset)
        for mode in MODES:
            await_capacity('zarr_' + mode + '_' + dataset)
            run_script('run_tpk_dask.py', ['--dataset', dataset, '--mode', mode], 'zarr_' + mode + '_' + dataset)
    stop_dask()


def compile_sedona():
    if 'compile_sedona' in completed():
        return
    classes = ROOT / 'classes'
    classes.mkdir(exist_ok=True)
    subprocess.run([str(BASE / 'jdk17/bin/javac'), '-cp',
                    str(BASE / 'spark356/jars/*') + ':' + str(BASE / 'sedona-jars/*'),
                    '-d', str(classes), str(ROOT / 'SedonaTPK.java')], check=True)
    subprocess.run([str(BASE / 'jdk17/bin/jar'), 'cf', str(ROOT / 'sedona-tpk.jar'), '-C', str(classes), '.'], check=True)
    mark('compile_sedona')


def spark_alive():
    try:
        with socket.create_connection(('192.0.2.64', 7078), timeout=3):
            return True
    except OSError:
        return False


def start_spark():
    if spark_alive():
        mark('spark_up')
        return
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda ip: _ignore(node, ip, 'stop', 'spark_worker'), WORKERS))
    _ignore(local_stop, 'spark_master')
    await_capacity('start_spark')
    subprocess.run([CLUSTER_PYTHON, str(ROOT / 'node_service.py'), 'start', 'spark_master'], check=True)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda ip: node(ip, 'start', 'spark_worker'), WORKERS))
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if spark_alive():
            break
        time.sleep(2)
    else:
        raise RuntimeError('Spark master did not come up')
    time.sleep(15)
    mark('spark_up')


def stop_spark():
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda ip: _ignore(node, ip, 'stop', 'spark_worker'), WORKERS))
    _ignore(local_stop, 'spark_master')
    mark('spark_down')


def sedona_phase():
    compile_sedona()
    start_spark()
    env = os.environ.copy()
    env.update(JAVA_HOME=str(BASE / 'jdk17'), SPARK_HOME=str(BASE / 'spark356'), SPARK_LOCAL_IP='192.0.2.64')
    env.pop('SPARK_DIST_CLASSPATH', None)
    jars = ':'.join(str(p) for p in (BASE / 'sedona-jars').glob('*.jar'))
    for dataset in DATASETS:
        label = 'sedona_' + dataset
        if label in completed():
            continue
        if dataset == 'gleam':
            wait_restore(RESTORE_WAIT['gleam'], 'gleam sedona')
        if dataset == 'ecmwf':
            wait_restore(RESTORE_WAIT['ecmwf_sedona'], 'ecmwf sedona')
        await_capacity(label)
        state(status='RUNNING', current=label)
        command = [str(BASE / 'spark356/bin/spark-submit'), '--master', 'spark://192.0.2.64:7078',
                   '--class', 'SedonaTPK', '--driver-class-path', jars,
                   '--conf', 'spark.executor.extraClassPath=' + jars,
                   '--executor-cores', '3', '--executor-memory', '8g', '--total-executor-cores', '48',
                   '--driver-memory', '4g', '--conf', 'spark.driver.host=192.0.2.64',
                   '--conf', 'spark.driver.bindAddress=192.0.2.64',
                   '--conf', 'spark.cores.max=48', '--conf', 'spark.executor.instances=16',
                   str(ROOT / 'sedona-tpk.jar'), str(ROOT), MANIFEST_DIR[dataset], dataset,
                   ','.join(MODES), '2', ','.join(WORKERS)]
        with (ROOT / (label + '.log')).open('a') as stream:
            subprocess.run(command, env=env, stdout=stream, stderr=subprocess.STDOUT, check=True)
        mark(label)
    stop_spark()


def summary():
    rows = []
    for path in sorted(ROOT.glob('*_results.jsonl')):
        for line in path.read_text().splitlines():
            if line.strip():
                record = json.loads(line)
                rows.append({k: record.get(k) for k in ('method', 'dataset', 'mode', 'run', 'status',
                                                        'until_final_result_seconds', 'until_final_120_month_results_seconds')})
    final_rows = []
    for path in sorted(ROOT.glob('sedona_*_state.json')):
        final_rows.append({'file': path.name, **json.loads(path.read_text())})
    (ROOT / 'tpk_summary.json').write_text(json.dumps({'runs': rows, 'sedona_summaries': final_rows}, indent=1))
    print('TPK_SUMMARY ' + json.dumps({'run_records': len(rows), 'sedona_states': len(final_rows)}), flush=True)
    mark('summary')


def main():
    assert BASE.is_dir() and (BASE / 'spark356').is_dir() and (BASE / 'sedona-jars').is_dir(), 'runtime missing'
    RESTORE_STATES.mkdir(exist_ok=True)
    state(status='RUNNING')
    try:
        deploy()
        stop_flink()
        zarr_phase()
        sedona_phase()
        restore_services()
        summary()
        state(status='COMPLETE')
        print('TPK_PIPELINE_COMPLETE', flush=True)
    except Exception as error:
        state(status='FAILED', error=str(error))
        try:
            restore_services()
        except Exception as restore_error:
            state(restore_error=str(restore_error))
        raise


if __name__ == '__main__':
    main()
