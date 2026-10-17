"""Start and stop only trial-owned process groups, with PID identity checks."""
import argparse
import glob
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
BASE = Path('/opt/mgsi-modern')
ROOT = Path(__file__).resolve().parent
IP = socket.gethostbyname(socket.gethostname())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['start','stop'])
    parser.add_argument('role', choices=['dask_scheduler','dask_worker','spark_master','spark_worker'])
    parser.add_argument('--nthreads',type=int,choices=[1,3],default=3)
    args = parser.parse_args()
    (BASE / 'run').mkdir(exist_ok=True)
    pin = BASE / 'run' / (args.role + '.json')
    if args.action == 'stop':
        if not pin.exists():
            return
        state = json.loads(pin.read_text())
        proc = Path('/proc') / str(state['pid'])
        if proc.exists():
            ticks = (proc / 'stat').read_text().split()[21]
            command = (proc / 'cmdline').read_bytes().replace(b'\0', b' ').decode()
            assert state.get('owner') == str(ROOT) and ticks == state['start_ticks'] and '/opt/mgsi-modern/' in command, (args.role, 'PID identity changed')
            os.killpg(state['pid'], signal.SIGTERM)
            deadline = time.monotonic()+30
            while proc.exists() and time.monotonic()<deadline:
                try:
                    if (proc/'stat').read_text().split()[2] == 'Z':
                        break
                except FileNotFoundError:
                    break
                time.sleep(0.2)
        pin.unlink()
        print(json.dumps({'stopped_role':args.role,'host':IP}))
        return
    assert not pin.exists(), 'Trial role already registered: '+args.role
    env = os.environ.copy()
    env.update(PYTHONPATH=str(ROOT), OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1',
               HADOOP_USER_NAME='root', LIBHDFS_OPTS='-Xmx256m -XX:ActiveProcessorCount=3',
               DASK_DISTRIBUTED__WORKER__MEMORY__SPILL='false',
               DASK_DISTRIBUTED__WORKER__MEMORY__TARGET='false',
               DASK_DISTRIBUTED__WORKER__MEMORY__PAUSE='0.80',
               DASK_DISTRIBUTED__WORKER__MEMORY__TERMINATE='0.90')
    python = str(BASE / 'python312/bin/python')
    if args.role.startswith('dask'):
        env['JAVA_HOME']='/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64'
        # Scheduler deserialization also reconstructs pyarrow HDFS objects.
        # JNI needs explicit jars; it does not expand Hadoop's wildcards.
        source = env.get('HADOOP_CLASSPATH') or env.get('CLASSPATH')
        if not source:
            # Started outside env.sh (e.g. local scheduler start): compute it.
            source = subprocess.run(['/app/hadoop-2.7.2/bin/hadoop', '--config', '/opt/mgsi-cluster/hadoop-conf', 'classpath'],
                                    capture_output=True, text=True, timeout=60).stdout.strip()
        parts = source.split(':')
        env['CLASSPATH'] = ':'.join(item for part in parts for item in (glob.glob(part) if '*' in part else [part]))
        # libhdfs.so must be locatable when pickled HDFS objects are rebuilt.
        hadoop_native = '/app/hadoop-2.7.2/lib/native'
        env['ARROW_LIBHDFS_DIR'] = hadoop_native
        env['LD_LIBRARY_PATH'] = hadoop_native + ':' + env.get('LD_LIBRARY_PATH', '')
        if args.role=='dask_scheduler':
            command=[python,'-m','distributed.cli.dask_scheduler','--host',IP,'--port','38786','--no-dashboard']
        else:
            command=[python,'-m','distributed.cli.dask_worker','tcp://192.0.2.64:38786','--host',IP,'--nworkers','4','--nthreads',str(args.nthreads),'--memory-limit','8GiB','--no-dashboard','--local-directory',str(ROOT/'tmp/dask')]
    else:
        env.update(JAVA_HOME=str(BASE/'jdk17'), SPARK_HOME=str(BASE/'spark356'), SPARK_LOCAL_IP=IP,
                   SPARK_DAEMON_MEMORY='512m', SPARK_LOCAL_DIRS=str(ROOT/'tmp/spark'))
        env.pop('SPARK_DIST_CLASSPATH', None)
        executable=str(BASE/'spark356/bin/spark-class')
        if args.role=='spark_master':
            command=[executable,'org.apache.spark.deploy.master.Master','--host',IP,'--port','7078','--webui-port','8082']
        else:
            command=[executable,'org.apache.spark.deploy.worker.Worker','--host',IP,'--webui-port','8083','--cores','12','--memory','32g','--work-dir',str(ROOT/'tmp/spark-work'),'spark://192.0.2.64:7078']
    with (ROOT/(args.role+'_'+IP+'.log')).open('a') as stream:
        process=subprocess.Popen(command, cwd=str(ROOT), env=env, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
    ticks=(Path('/proc')/str(process.pid)/'stat').read_text().split()[21]
    pin.write_text(json.dumps({'pid':process.pid,'start_ticks':ticks,'command':command,'host':IP,'owner':str(ROOT)},indent=2))
    time.sleep(1)
    assert process.poll() is None, 'Service failed: '+args.role
    print(json.dumps({'started_role':args.role,'host':IP,'pid':process.pid}),flush=True)


if __name__=='__main__':
    main()
