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
from scipy.spatial import KDTree

ROOT = Path(__file__).resolve().parent
REST = 'http://192.0.2.64:8081'
PORT = 38766


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
        while len(rows) < 72:
            event = self.events.get(timeout=max(1, deadline-time.monotonic()))
            if event['type'] in ('source', 'worker_ready'):
                continue
            assert event['type'] == 'result', event
            assert event['query_id'] == query_id and event['route'] not in rows, event
            rows[event['route']] = event
        return [rows[i] for i in range(72)]


def locality_assignment(meta, connections):
    groups = defaultdict(list)
    for route, info in connections.workers.items():
        groups[(info['host'], info['jvm'])].append(route)
    assert len(groups) == 24 and all(len(v) == 3 for v in groups.values())
    physical = [{int(row[0]): row for row in f['physical_chunks']} for f in meta['files']]
    load = {key: 0.0 for key in groups}
    route_load = {route: 0.0 for route in connections.workers}
    assignments = {}
    records = []
    # Persistent ownership keeps metadata, physical-plane and KD caches reusable.
    for entry in meta['entries']:
        file = meta['files'][entry['file_id']]
        pc = file['physical_shape']
        pg = file['physical_grid']
        weights = defaultdict(float)
        fetched = set()
        cost = 0
        for ti in file['time_indices']:
            for rc in range(entry['rows'][0]//pc[1], (entry['rows'][1]-1)//pc[1]+1):
                for cc in range(entry['cols'][0]//pc[2], (entry['cols'][1]-1)//pc[2]+1):
                    key = (ti//pc[0]*pg[1]+rc)*pg[2]+cc
                    if key in fetched:
                        continue
                    fetched.add(key)
                    _, offset, size, mask = physical[entry['file_id']][key]
                    cost += size
                    for block in file['hdfs_blocks']:
                        overlap = max(0, min(offset+size, block['offset']+block['length']) - max(offset, block['offset']))
                        if overlap:
                            for host in block['hosts']:
                                weights[host] += overlap
        # Include the persistent selected-plane footprint so ocean chunks do
        # not overload one JVM just because their compressed bytes are small.
        cost += len(fetched)*pc[1]*pc[2]*4
        cost += (entry['rows'][1]-entry['rows'][0])*(entry['cols'][1]-entry['cols'][0])*len(file['time_indices'])/10
        maximum = max(weights.values()) if weights else 0
        choices = [group for group in groups if not weights or weights[group[0]] >= maximum*0.70]
        if not choices:
            choices = list(groups)
        target = min(choices, key=lambda group: (load[group], group))
        route = min(groups[target], key=lambda r: (route_load[r], r))
        assignments[entry['entry_id']] = route
        load[target] += cost
        route_load[route] += cost
        records.append({'entry_id': entry['entry_id'], 'route': route, 'host': target[0], 'jvm': target[1],
                        'estimated_cost': cost, 'local_bytes_weight': weights[target[0]], 'best_local_bytes_weight': maximum})
    (ROOT / 'locality_assignment.json').write_text(json.dumps(records, indent=2))
    return assignments


def main():
    overview=get('/overview')
    assert overview['jobs-running'] == 0 and overview['slots-available'] == 72,overview
    assert not (ROOT/'results.jsonl').exists(), 'Retain/archive previous records before a new run.'
    meta_path = ROOT / 'physical_index.with_blocks.json'
    meta = json.loads(meta_path.read_text())
    golden = {row['case']: row['monthly_values'] for row in json.loads((ROOT/'golden_results.json').read_text())}
    entries = meta['entries']
    # The thesis uses the upper-left representative; exact extents guard pruning.
    representatives = [(e['lon_min'], e['lat_max']) for e in entries]
    tree = KDTree(np.asarray(representatives))
    max_diagonal = max(math.hypot(e['lon_max']-e['lon_min'], e['lat_max']-e['lat_min']) for e in entries)
    pin = {'files': {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
                     for name in ('ResidentMGSI.java', 'resident-mgsi.jar', 'run_resident.py', 'physical_index.with_blocks.json', 'golden_results.json')},
           'scope': meta['scope'], 'cache_per_jvm_bytes': 512*1024**2,
           'protocol': 'Resident Flink/JVM; first query per region clears selected-plane cache, then five repeated queries; OS/HDFS cache uncontrolled; no aggregate-result cache; initialization timed separately.'}
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
        while len(connections.workers) < 72 or connections.source is None:
            assert time.monotonic() < deadline, (len(connections.workers), get('/jobs/'+job_id))
            assert get('/jobs/'+job_id)['state'] not in ('FAILED', 'CANCELED', 'FINISHED')
            time.sleep(0.5)
        connections.send([{'type':'query', 'route': route, 'query_id':'initialization_barrier', 'entries': [], 'box': [-45,0,-90,90]} for route in range(72)])
        connections.results('initialization_barrier')
        ready = time.perf_counter()
        initialization = {'job_id': job_id, 'until_all_72_return_paths_ready_seconds': ready-started,
                          'workers': connections.workers, 'physical_index_preparation_seconds_separate': meta['preparation_seconds'],
                          'protocol': pin['protocol']}
        (ROOT/'initialization.json').write_text(json.dumps(initialization, indent=2))
        assignment = locality_assignment(meta, connections)
        print('RESIDENT_READY ' + json.dumps({'job_id':job_id, 'initialization_seconds':ready-started, 'workers':72}), flush=True)
        for region in range(1,9):
            box = meta['boxes'][str(region)]
            expected = golden['gleam_1981_1990_spatial_R'+str(region)]
            for repeat in range(6):
                query_id = 'R{}_{}'.format(region, repeat)
                timer = time.perf_counter()
                center = ((box['lon_min']+box['lon_max'])/2, (box['lat_min']+box['lat_max'])/2)
                radius = math.hypot(box['lon_max']-box['lon_min'], box['lat_max']-box['lat_min'])/2 + max_diagonal
                ids = tree.query_ball_point(center,radius+1e-9)
                predicate = lambda e: e['lat_max']>=box['lat_min'] and e['lat_min']<=box['lat_max'] and e['lon_max']>=box['lon_min'] and e['lon_min']<=box['lon_max']
                selected = sorted(i for i in ids if predicate(entries[i]))
                expected_ids = [i for i,e in enumerate(entries) if predicate(e)]
                assert selected == expected_ids
                byroute = [[] for _ in range(72)]
                for entry_id in selected:
                    byroute[assignment[entry_id]].append(entry_id)
                requests = [{'type':'query','route':route,'query_id':query_id,'entries':byroute[route],
                             'box':[box['lat_min'],box['lat_max'],box['lon_min'],box['lon_max']],
                             'clear_epoch':query_id if repeat==0 else ''} for route in range(72)]
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
                assert monthly.keys() == expected.keys()
                for month in monthly:
                    assert monthly[month]['count'] == expected[month]['count'], (query_id,month,'count',monthly[month],expected[month])
                    assert math.isclose(monthly[month]['sum'], expected[month]['sum'], rel_tol=1e-9, abs_tol=1e-6), (query_id,month,'sum',monthly[month],expected[month])
                record = {'status':'PASS','query_id':query_id,'job_id':job_id,'region':region,'repeat':repeat,
                          'mode':'selected_plane_cache_cleared' if repeat==0 else 'repeated_resident_query',
                          'until_final_120_month_results_seconds':elapsed,'monthly_values':monthly,
                          'physical_read_bytes':sum(row['physical_read_bytes'] for row in rows),
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
    main()
