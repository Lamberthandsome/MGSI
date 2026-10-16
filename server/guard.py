"""Fail closed before the author's 90% filesystem limit; never deletes data."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json
import math
import shlex
import subprocess
import time

ROOT = Path(__file__).resolve().parent
HOSTS = ['192.0.2.64','192.0.2.65','192.0.2.66','192.0.2.71','192.0.2.72']
START_LIMIT = 89.80
STOP_LIMIT = 89.85
MEMORY_FLOOR = 8 * 1024**3
WORKER_WRITE_BUDGET = 512 * 1024**2
MASTER_WRITE_BUDGET = 2 * 1024**3
INTERVAL = 2.0

NODE_CODE = r'''
import json, os, pathlib, xml.etree.ElementTree as ET
root = pathlib.Path(__ROOT__)
names = ['/', '/tmp', '/opt/mgsi-modern', str(root)]
for name in ['/app/hadoop-2.7.2/etc/hadoop/hdfs-site.xml','/opt/mgsi-cluster/hadoop/etc/hadoop/hdfs-site.xml']:
    p = pathlib.Path(name)
    if not p.exists(): continue
    for prop in ET.parse(str(p)).getroot().findall('property'):
        if prop.findtext('name') == 'dfs.datanode.data.dir':
            for value in prop.findtext('value','').split(','):
                value = value.strip().replace('file://','')
                if value: names.append(value)
disks=[]; seen=set()
for name in names:
    p=pathlib.Path(name)
    while not p.exists() and p != p.parent: p=p.parent
    device=p.stat().st_dev
    if device in seen: continue
    seen.add(device); s=os.statvfs(str(p)); unit=s.f_frsize
    used=(s.f_blocks-s.f_bfree)*unit; available=s.f_bavail*unit
    disks.append({'path':str(p),'device':device,'used_percent':100*used/(used+available),
        'available_bytes':available,'used_bytes':used,'total_bytes':s.f_blocks*unit})
mem={}
for line in pathlib.Path('/proc/meminfo').read_text().splitlines():
    k,v=line.split(':',1)
    if k=='MemAvailable': mem[k]=int(v.strip().split()[0])*1024
size=0
if root.exists():
    for base, dirs, files in os.walk(str(root)):
        for name in files:
            try: size+=os.lstat(os.path.join(base,name)).st_blocks*512
            except FileNotFoundError: pass
print(json.dumps({'ok':True,'disks':disks,'memory_available_bytes':mem['MemAvailable'],
    'new_run_allocated_bytes':size}))
'''

class ResourceLimit(RuntimeError): pass

def one(host):
    code=NODE_CODE.replace('__ROOT__',repr(str(ROOT)))
    if host==HOSTS[0]:
        argv=['/opt/mgsi-modern/python312/bin/python','-B','-']
    else:
        argv=['ssh','-T','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes',
              '-o','ConnectTimeout=5','-o','ServerAliveInterval=3','-o','ServerAliveCountMax=1',
              'root@'+host,'/opt/mgsi-modern/python312/bin/python -B -']
    try:
        p=subprocess.run(argv,input=code,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=12)
        if p.returncode: return {'host':host,'ok':False,'error':p.stderr[-1000:]}
        return dict(json.loads(p.stdout),host=host)
    except Exception as exc: return {'host':host,'ok':False,'error':str(exc)}

def snapshot():
    with ThreadPoolExecutor(max_workers=5) as pool: return list(pool.map(one,HOSTS))

def reasons(rows, running=True):
    issues=[]
    if {r.get('host') for r in rows} != set(HOSTS): issues.append('Incomplete five-node snapshot')
    limit=STOP_LIMIT if running else START_LIMIT
    for row in rows:
        host=row.get('host','unknown')
        if not row.get('ok'):
            issues.append(host+' unreachable/unknown: '+row.get('error','')); continue
        if not row.get('disks'): issues.append(host+' has no filesystem measurements')
        for disk in row.get('disks',[]):
            percent=disk.get('used_percent',float('nan'))
            if not math.isfinite(percent) or percent>=limit:
                issues.append('{} {} used={:.5f}% >= {:.2f}%'.format(host,disk['path'],percent,limit))
        if row.get('memory_available_bytes',0)<MEMORY_FLOOR:
            issues.append(host+' available memory below 8 GiB')
        budget=MASTER_WRITE_BUDGET if host==HOSTS[0] else WORKER_WRITE_BUDGET
        if row.get('new_run_allocated_bytes',budget+1)>budget:
            issues.append(host+' new-run files exceeded bounded write budget')
    return issues

def check(running=True):
    rows=snapshot(); problems=reasons(rows,running)
    record={'time':time.time(),'nodes':rows,'issues':problems,'running':running}
    with (ROOT/'resource_samples.jsonl').open('a') as out: out.write(json.dumps(record)+'\n')
    tmp=ROOT/'resource_latest.tmp'; tmp.write_text(json.dumps(record,indent=2)); tmp.replace(ROOT/'resource_latest.json')
    if problems: raise ResourceLimit('; '.join(problems))
    return rows

def test_policy():
    def healthy():
        return [{'host':h,'ok':True,'disks':[{'path':'/','used_percent':89.76}],
            'memory_available_bytes':16*1024**3,'new_run_allocated_bytes':0} for h in HOSTS]
    assert not reasons(healthy(),running=False)
    rows=healthy(); rows[-1]['disks'][0]['used_percent']=89.85
    assert reasons(rows) and '89.85' in reasons(rows)[0]
    rows=healthy(); rows[1]['ok']=False; rows[1]['error']='SSH timeout'
    assert reasons(rows)
    rows=healthy(); rows[2]['memory_available_bytes']=7*1024**3
    assert reasons(rows)
    rows=healthy(); rows[3]['new_run_allocated_bytes']=WORKER_WRITE_BUDGET+1
    assert reasons(rows)
    rows=healthy(); rows[-1]['disks'].append({'path':'/data','used_percent':90.0})
    assert reasons(rows)
    rows=healthy(); rows[2]['disks'][0]['used_percent']=float('nan')
    assert reasons(rows)
    print('GUARD_POLICY_PASS: pre-limit stop, node loss, memory, bounded writes, data mount, invalid measurements')

if __name__=='__main__': test_policy()
