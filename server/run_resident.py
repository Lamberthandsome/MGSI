"""Run the real ten-year cases on one resident Flink job and verify all months."""
import hashlib
import json
import math
import queue
import re
import socket
import subprocess
import threading
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

import numpy as np
from chunk_tree import Chunk, ChunkTree

ROOT = Path(__file__).resolve().parent
REST = 'http://192.0.2.64:8081'
PORT = 38769


def get(path):
    with urllib.request.urlopen(REST + path, timeout=20) as stream:
        return json.load(stream)


class Connections:
    def __init__(self):
        self.server = socket.socket()
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('192.0.2.64', PORT))
        self.server.listen(128)
        self.events = queue.Queue()
        self.source = None
        self.workers = {}
        threading.Thread(target=self.accept, daemon=True).start()

    def accept(self):
        while True:
            try:
                sock, addr = self.server.accept()
            except OSError:
                return
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            threading.Thread(target=self.read, args=(sock,), daemon=True).start()

    def read(self, sock):
        try:
            with sock.makefile('r', encoding='utf-8') as stream:
                hello = json.loads(stream.readline())
                if hello['type'] == 'source':
                    self.source = sock.makefile('w', encoding='utf-8', buffering=1)
                    self.events.put(hello)
                    while sock.recv(1):
                        pass
                else:
                    assert hello['type'] == 'worker_ready'
                    self.workers[hello['route']] = hello
                    self.events.put(hello)
                    for line in stream:
                        self.events.put(json.loads(line))
        except Exception as exc:
            self.events.put({'type': 'connection_error', 'error': str(exc)})

    def send(self, requests):
        self.source.write(''.join(json.dumps(request, separators=(',', ':')) + '\n' for request in requests))
        self.source.flush()

    def results(self, query_id):
        rows = {}
        deadline = time.monotonic() + 600
        while len(rows) < 48:
            event = self.events.get(timeout=max(1, deadline-time.monotonic()))
            if event['type'] in ('source', 'worker_ready'):
                continue
            assert event['type'] == 'result', event
            assert event['query_id'] == query_id and event['route'] not in rows, event
            rows[event['route']] = event
        return [rows[i] for i in range(48)]


def locality_assignment(meta, connections):
    groups=defaultdict(list)
    for route,info in connections.workers.items():
        groups[(info['host'],info['jvm'])].append(route)
    assert len(groups)==16 and all(len(v)==3 for v in groups.values())
    spatial=defaultdict(list)
    for entry in meta['entries']:
        spatial[(entry['file_id'],entry['rows'][0]//450,entry['cols'][0]//720)].append(entry)
    physical=[{int(r[0]):r for r in f['physical_chunks']} for f in meta['files']]
    load={g:0.0 for g in groups}
    route_load={r:0.0 for r in connections.workers}
    assignment={}
    records=[]
    for group_key,entries in sorted(spatial.items()):
        f=meta['files'][group_key[0]]
        pc,pg=f['physical_shape'],f['physical_grid']
        r0=min(e['rows'][0] for e in entries);r1=max(e['rows'][1] for e in entries)
        c0=min(e['cols'][0] for e in entries);c1=max(e['cols'][1] for e in entries)
        weights=defaultdict(float);cost=0;keys=set()
        for ti in f['time_indices']:
            for rc in range(r0//pc[1],(r1-1)//pc[1]+1):
                for cc in range(c0//pc[2],(c1-1)//pc[2]+1):
                    key=(ti//pc[0]*pg[1]+rc)*pg[2]+cc
                    if key in keys:
                        continue
                    keys.add(key)
                    _,offset,size,mask=physical[f['file_id']][key]
                    cost+=size
                    for block in f['hdfs_blocks']:
                        overlap=max(0,min(offset+size,block['offset']+block['length'])-max(offset,block['offset']))
                        if overlap:
                            for host in block['hosts']:
                                weights[host]+=overlap
        cost+=len(keys)*pc[1]*pc[2]*4
        maximum=max(weights.values()) if weights else 0
        choices=[g for g in groups if not weights or weights[g[0]]>=maximum*0.70] or list(groups)
        owner=min(choices,key=lambda g:(load[g],g))
        load[owner]+=cost
        for entry in entries:
            route=min(groups[owner],key=lambda r:(route_load[r],r))
            route_load[route]+=(entry['rows'][1]-entry['rows'][0])*(entry['cols'][1]-entry['cols'][0])*len(f['time_indices'])
            assignment[entry['entry_id']]=route
            records.append({'entry_id':entry['entry_id'],'spatial_group':list(group_key),'route':route,'host':owner[0],'jvm':owner[1],
                            'group_estimated_cost':cost,'local_bytes_weight':weights[owner[0]],'best_local_bytes_weight':maximum})
    (ROOT/'locality_assignment.json').write_text(json.dumps(records,indent=2))
    return assignment


def main():
    overview=get('/overview')
    assert overview['jobs-running'] == 0 and overview['slots-available'] == 48,overview
    assert not (ROOT/'results.jsonl').exists(), 'Retain/archive previous records before a new run.'
    meta_path = ROOT / 'physical_index.with_blocks.json'
    meta = json.loads(meta_path.read_text())
    golden = {row['case']: row['monthly_values'] for row in json.loads((ROOT/'golden_results.json').read_text())}
    entries = meta['entries']
    # Adopted Global Index rules: centroid pivot at every subtree, full chunk bounds.
    index_started = time.perf_counter()
    chunks = [Chunk(str(i), (e['lon_min'], e['lat_min']),
        (e['lon_max'], e['lat_max']), (e['lon_min'], e['lat_min'])) for i,e in enumerate(entries)]
    tree = ChunkTree(chunks)
    (ROOT/'global_index_cost.json').write_text(json.dumps({'construction_seconds':time.perf_counter()-index_started,
        'chunks':len(chunks),'implementation_sha256':hashlib.sha256((ROOT/'chunk_tree.py').read_bytes()).hexdigest()}))
    pin = {'files': {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
                     for name in ('ResidentMGSI.java', 'resident-mgsi.jar', 'run_resident.py', 'physical_index.with_blocks.json', 'golden_results.json')},
           'scope': meta['scope'], 'cache_per_jvm_bytes': 512*1024**2,
           'global_index_implementation': 'P17/P18 adopted centroid/subtree-bound specification',
           'protocol': 'Four workers; 16 JVMs x 3 subtasks; 48 cores; 8 GiB process configuration per JVM; resident engine with prepared metadata and indexes; one warmup plus five measured queries per R1-R8; application plane cache cleared before every query; no persisted aggregate results; OS/HDFS caches uncontrolled and disclosed; initialization and index preparation separate.'}
    connections = Connections()
    (ROOT/'run_pin.json').write_text(json.dumps(pin, indent=2))
    started = time.perf_counter()
    submitted = subprocess.run(['/opt/mgsi-cluster/flink/bin/flink', 'run', '-d', '-m', '192.0.2.64:8081', '-c', 'ResidentMGSI', str(ROOT/'resident-mgsi.jar'), '192.0.2.64', str(PORT), str(meta_path)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    (ROOT/'submission.log').write_text(submitted.stdout)
    assert submitted.returncode == 0, submitted.stdout
    match = re.search(r'JobID\s+([0-9a-f]{32})', submitted.stdout)
    assert match, submitted.stdout
    job_id = match.group(1)
    state = {'status': 'INITIALIZING', 'job_id': job_id}
    (ROOT/'state.json').write_text(json.dumps(state, indent=2))
    try:
        deadline = time.monotonic()+240
        while len(connections.workers) < 48 or connections.source is None:
            assert time.monotonic() < deadline, (len(connections.workers), get('/jobs/'+job_id))
            assert get('/jobs/'+job_id)['state'] not in ('FAILED', 'CANCELED', 'FINISHED')
            time.sleep(0.5)
        connections.send([{'type':'query', 'route': route, 'query_id':'initialization_barrier', 'entries': [], 'box': [-45,0,-90,90]} for route in range(48)])
        initial_rows=connections.results('initialization_barrier')
        assert all(r['prepared_header_bytes_per_jvm']==80 and r['prepared_spatial_tree_templates_per_jvm']==500 for r in initial_rows)
        ready = time.perf_counter()
        initialization = {'job_id': job_id, 'until_all_72_return_paths_ready_seconds': ready-started,
                          'workers': connections.workers, 'header_bytes_read_in_initialization':16*80, 'spatial_templates_prepared_per_jvm':500, 'physical_index_preparation_seconds_separate': meta['preparation_seconds'],
                          'protocol': pin['protocol']}
        (ROOT/'initialization.json').write_text(json.dumps(initialization, indent=2))
        owner_start=time.perf_counter()
        assignment = locality_assignment(meta, connections)
        initialization['ownership_preparation_seconds']=time.perf_counter()-owner_start
        initialization['until_query_service_ready_seconds']=time.perf_counter()-started
        (ROOT/'initialization.json').write_text(json.dumps(initialization,indent=2))
        print('RESIDENT_READY ' + json.dumps({'job_id':job_id, 'initialization_seconds':ready-started, 'workers':48}), flush=True)
        for region in range(1,9):
            box = meta['boxes'][str(region)]
            expected = golden['gleam_1981_1990_spatial_R'+str(region)]
            for repeat in range(6):
                query_id = 'R{}_{}'.format(region, repeat)
                timer = time.perf_counter()
                selected = [int(identifier) for identifier in tree.query(
                    (box['lon_min'],box['lat_min']), (box['lon_max'],box['lat_max']))]
                predicate = lambda e: e['lat_max']>=box['lat_min'] and e['lat_min']<=box['lat_max'] and e['lon_max']>=box['lon_min'] and e['lon_min']<=box['lon_max']
                byroute = [[] for _ in range(48)]
                for entry_id in selected:
                    byroute[assignment[entry_id]].append(entry_id)
                requests = [{'type':'query','route':route,'query_id':query_id,'entries':byroute[route],
                             'box':[box['lat_min'],box['lat_max'],box['lon_min'],box['lon_max']],
                             'clear_epoch':query_id} for route in range(48)]
                connections.send(requests)
                rows = connections.results(query_id)
                monthly = {}
                for row in rows:
                    for month,value in row['monthly'].items():
                        aggregate = monthly.setdefault(month, {'sum':0.0,'count':0})
                        aggregate['sum'] += value['sum']
                        aggregate['count'] += value['count']
                for month,value in monthly.items():
                    value['mean'] = value['sum']/value['count'] if value['count'] else None
                elapsed = time.perf_counter()-timer
                assert set(selected) == {i for i,e in enumerate(entries) if predicate(e)}
                assert {row['host'] for row in rows if row['monthly']} == {'192.0.2.65','192.0.2.66','192.0.2.71','192.0.2.72'}
                assert monthly.keys() == expected.keys()
                for month in monthly:
                    assert monthly[month]['count'] == expected[month]['count'], (query_id,month,'count',monthly[month],expected[month])
                    assert math.isclose(monthly[month]['sum'], expected[month]['sum'], rel_tol=1e-9, abs_tol=1e-6), (query_id,month,'sum',monthly[month],expected[month])
                record = {'status':'PASS','query_id':query_id,'job_id':job_id,'region':region,'repeat':repeat,
                          'mode':'warmup' if repeat==0 else 'measured', 'measured_repeat':repeat,
                          'cross_query_application_data_cache':'cleared_before_every_query',
                          'until_final_120_month_results_seconds':elapsed,'monthly_values':monthly,
                          'physical_read_bytes':sum(row['physical_read_bytes'] for row in rows),
                          'physical_read_requests':sum(row['physical_read_requests'] for row in rows),
                          'plane_cache_hits':sum(row['plane_cache_hits'] for row in rows),
                          'plane_cache_misses':sum(row['plane_cache_misses'] for row in rows),
                          'active_data_hosts':sorted({row['host'] for row in rows if row['monthly']}),
                          'worker_rows':rows,'protocol':pin['protocol']}
                with (ROOT/'results.jsonl').open('a') as stream:
                    stream.write(json.dumps(record,allow_nan=False)+'\n')
                print('RESIDENT_RESULT '+json.dumps({key:value for key,value in record.items() if key not in ('monthly_values','worker_rows','protocol')}), flush=True)
                state.update(status='RUNNING',last_completed=query_id)
                (ROOT/'state.json').write_text(json.dumps(state, indent=2))
        state['status']='COMPLETE'
    finally:
        if connections.source is not None:
            connections.send([{'type':'stop'}])
        deadline = time.monotonic()+60
        while get('/jobs/'+job_id)['state'] not in ('FINISHED','FAILED','CANCELED') and time.monotonic()<deadline:
            time.sleep(0.5)
        job=get('/jobs/'+job_id)
        if job['state'] not in ('FINISHED','FAILED','CANCELED'):
            subprocess.run(['/opt/mgsi-cluster/flink/bin/flink','cancel','-m','192.0.2.64:8081',job_id],check=True)
            job=get('/jobs/'+job_id)
        for name,path in [('job','/jobs/'+job_id),('plan','/jobs/'+job_id+'/plan'),('exceptions','/jobs/'+job_id+'/exceptions'),('environment_final','/overview')]:
            (ROOT/(name+'.json')).write_text(json.dumps(get(path),indent=2))
        state['job_state']=job['state']
        (ROOT/'state.json').write_text(json.dumps(state,indent=2))
        connections.server.close()
    assert state['status']=='COMPLETE' and state['job_state']=='FINISHED',state
    print('RESIDENT_COMPLETE '+json.dumps(state),flush=True)


if __name__ == '__main__':
    import signal
    def interrupted(signum,frame): raise InterruptedError('Resource supervisor stopped this query job')
    signal.signal(signal.SIGTERM,interrupted)
    main()
