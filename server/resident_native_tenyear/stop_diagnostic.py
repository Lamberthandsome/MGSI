"""Stop only this experiment and retain the complete pre-fix diagnostic."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parent
state = json.loads((ROOT/'state.json').read_text())
job_id = state['job_id']
assert len(job_id) == 32 and all(c in '0123456789abcdef' for c in job_id)


def get(path):
    with urllib.request.urlopen('http://192.0.2.64:8081'+path,timeout=20) as stream:
        return json.load(stream)


job = get('/jobs/'+job_id)
assert job['name'] == 'MGSI resident native ten-year GLEAM reconstruction'
if job['state'] == 'RUNNING':
    subprocess.run(['/opt/mgsi-cluster/flink/bin/flink','cancel','-m','192.0.2.64:8081',job_id],check=True)
    for attempt in range(120):
        job = get('/jobs/'+job_id)
        if job['state'] == 'CANCELED':
            break
        time.sleep(0.5)
    assert job['state'] == 'CANCELED',job
assert job['state'] in ('CANCELED','FINISHED','FAILED'),job
stopped = []
for process in Path('/proc').iterdir():
    if not process.name.isdigit():
        continue
    try:
        args = (process/'cmdline').read_bytes().split(b'\0')
        cwd = (process/'cwd').resolve()
        if len(args)>2 and Path(os.fsdecode(args[0])).name.startswith('python') and os.fsdecode(args[1])=='run_resident.py' and cwd == ROOT:
            pid=int(process.name)
            os.kill(pid,signal.SIGTERM)
            stopped.append(pid)
    except (OSError,PermissionError):
        continue
time.sleep(1)
for name,path in [('job','/jobs/'+job_id),('plan','/jobs/'+job_id+'/plan'),('exceptions','/jobs/'+job_id+'/exceptions')]:
    (ROOT/(name+'.json')).write_text(json.dumps(get(path),indent=2))
reason={'action':'retained_diagnostic_then_rerun','job_id':job_id,'job_state':job['state'],
        'stopped_coordinator_pids':stopped,'reason':'256 MiB plane cache plus IO-weight-only ownership caused cache thrashing in larger regions. Increase bounded per-JVM cache and include selected-plane memory in ownership cost.'}
(ROOT/'diagnostic_stop.json').write_text(json.dumps(reason,indent=2))
target=ROOT/'diagnostic_256mb'
assert not target.exists()
target.mkdir()
for path in ROOT.iterdir():
    if path.is_file() and path.suffix in ('.json','.jsonl','.java','.jar','.py','.log'):
        shutil.copy2(path,target/path.name)
# The early restart overwrote its own source/pin/log before the prior TCP
# listener was released. Preserve that failed restart and recover the actual
# first-run artifact from the unchanged worker copy; do not call these original
# frozen pins. The successful query records themselves were never overwritten.
(target/'ResidentMGSI.java').rename(target/'ResidentMGSI.restart.java')
(target/'run_resident.py').rename(target/'run_resident.restart.py')
(target/'resident-mgsi.jar').rename(target/'resident-mgsi.restart.jar')
(target/'run_pin.json').rename(target/'failed_restart_pin.json')
(target/'resident_run.log').rename(target/'restart_before_listener_release.log')
source=(target/'ResidentMGSI.restart.java').read_bytes().replace(b'limit=512L*1024*1024',b'limit=256L*1024*1024')
(target/'ResidentMGSI.java').write_bytes(source)
controller=(target/'run_resident.restart.py').read_text().replace("'cache_per_jvm_bytes': 512*1024**2", "'cache_per_jvm_bytes': 256*1024**2")
controller=controller.replace('        # Include the persistent selected-plane footprint so ocean chunks do\n        # not overload one JVM just because their compressed bytes are small.\n        cost += len(fetched)*pc[1]*pc[2]*4\n','')
(target/'run_resident.py').write_text(controller)
subprocess.run(['scp','-q','root@192.0.2.65:/opt/mgsi-experiments/20261006-resident-native/resident-mgsi.jar',str(target/'resident-mgsi.jar')],check=True)
import hashlib
(target/'diagnostic_version_reconstructed.json').write_text(json.dumps({'method':'Actual submitted jar recovered from unchanged worker copy; source bytes reconstructed by reversing the documented cache changes. Original progress log/pin overwritten by early failed restart; successful results retained.',
    'files':{name:hashlib.sha256((target/name).read_bytes()).hexdigest() for name in ('resident-mgsi.jar','ResidentMGSI.java','run_resident.py','results.jsonl')}},indent=2))
for name in ('results.jsonl','state.json','resident_run.log','submission.log','initialization.json','locality_assignment.json','job.json','plan.json','exceptions.json','run_pin.json','diagnostic_stop.json'):
    if (ROOT/name).exists():
        (ROOT/name).unlink()
print(json.dumps(reason),flush=True)
