"""Dataset-parameterized ablation driver (v2): seven variants on gleam|merra2.

Usage: python run_ablation.py gleam   (or merra2)

Variants (query-path toggles; initialization and prepared metadata identical for all):
  full          — driver KD-tree selection, per-entry KD bounds, batched prefetch + plane cache
  no_chunking   — whole file as selection/IO unit (entry_mode=file); still exact bbox masking
  no_global     — driver linear bbox predicate instead of the global KD tree
  no_local      — per-entry coordinate scan instead of the prepared per-entry KD tree
  no_inmemory   — no batched prefetch, no plane cache: per-block direct reads
  no_all_three  — linear selection + linear bounds + direct reads
  global_only   — tree selection + linear bounds + direct reads

Cases: temporal_yangtze (102-112E, 27-34N, the paper's temporal-query region),
temporal_global (full extent; both aggregate every monthly 15th of the archive)
and R1-R8 boxes.
Arms: cleared (plane cache cleared before every query, the main-table protocol)
      retained (cache cleared only at the warmup of each variant x case).
Every query result is checked against golden monthly sums/counts (rel_tol 1e-9).
"""
import hashlib
import json
import math
import queue
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from collections import defaultdict
from pathlib import Path

from chunk_tree import Chunk, ChunkTree

ROOT = Path(__file__).resolve().parent
REST = 'http://192.0.2.64:8081'
DATASET = sys.argv[1] if len(sys.argv) > 1 else 'gleam'
CONFIGS = {
    'gleam':  dict(prefix='gleam_1981_1990',  tile_rows=450, tile_cols=720, port=38791, files=10),
    'merra2': dict(prefix='merra2_1980_1989', tile_rows=182, tile_cols=288, port=38792, files=120),
}
CONFIG = CONFIGS[DATASET]
PORT = CONFIG['port']

YANGTZE = {'lat_min': 27.0, 'lat_max': 34.0, 'lon_min': 102.0, 'lon_max': 112.0}

VARIANTS = [
    ('full',         'tree',   {}),
    ('no_chunking',  'file',   {'entry_mode': 'file'}),
    ('no_global',    'linear', {}),
    ('no_local',     'tree',   {'bounds_mode': 'linear'}),
    ('no_inmemory',  'tree',   {'cache_mode': 'none'}),
    ('no_all_three', 'linear', {'bounds_mode': 'linear', 'cache_mode': 'none'}),
    ('global_only',  'tree',   {'bounds_mode': 'linear', 'cache_mode': 'none'}),
]
ARMS = [('cleared', None), ('retained', ['temporal_yangtze', 'temporal_global', 'R1', 'R8'])]
REPEATS = 6  # 1 warmup + 5 measured


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
        deadline = time.monotonic() + 900
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
        spatial[(entry['file_id'],entry['rows'][0]//CONFIG['tile_rows'],entry['cols'][0]//CONFIG['tile_cols'])].append(entry)
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


def file_assignment(meta, connections):
    """no_chunking: each whole file is one unit on one route; greedy by physical bytes."""
    routes = sorted(connections.workers)
    load = {r: 0.0 for r in routes}
    assignment = {}
    records = []
    files = []
    for f in meta['files']:
        bytes_total = sum(row[2] for row in f['physical_chunks'])
        files.append((f['file_id'], bytes_total))
    for file_id, bytes_total in sorted(files, key=lambda x: (-x[1], x[0])):
        route = min(routes, key=lambda r: (load[r], r))
        load[route] += bytes_total
        assignment[file_id] = route
        records.append({'file_id': file_id, 'route': route, 'physical_bytes': bytes_total})
    (ROOT/'file_assignment.json').write_text(json.dumps(records, indent=2))
    return assignment


def main():
    overview=get('/overview')
    assert overview['jobs-running'] == 0 and overview['slots-available'] == 48,overview
    assert not (ROOT/'results.jsonl').exists(), 'Retain/archive previous records before a new run.'
    meta_path = ROOT / 'physical_index.with_blocks.json'
    meta = json.loads(meta_path.read_text())
    assert len(meta['files']) == CONFIG['files']
    golden = {row['case']: row['monthly_values'] for row in json.loads((ROOT/'golden_results.json').read_text())}
    golden.update({row['case']: row['monthly_values'] for row in json.loads((ROOT/'golden_extra.json').read_text())})
    entries = meta['entries']
    expected_templates = len({(e['rows'][0], e['rows'][1], e['cols'][0], e['cols'][1]) for e in entries})

    lats = meta['files'][0]['lat']; lons = meta['files'][0]['lon']
    for f in meta['files']:
        assert f['lat'] == lats and f['lon'] == lons
    global_box = {'lat_min': min(lats), 'lat_max': max(lats), 'lon_min': min(lons), 'lon_max': max(lons)}
    cases = [('temporal_yangtze', YANGTZE), ('temporal_global', global_box)] + [
        ('R'+str(region), meta['boxes'][str(region)]) for region in range(1, 9)]
    golden_case = {name: CONFIG['prefix']+suffix for name, suffix in
                   (('temporal_yangtze', '_temporal_yangtze'), ('temporal_global', '_temporal_global'))}

    # Global index over tile entries (adopted centroid/subtree-bound specification).
    index_started = time.perf_counter()
    chunks = [Chunk(str(i), (e['lon_min'], e['lat_min']),
        (e['lon_max'], e['lat_max']), (e['lon_min'], e['lat_min'])) for i,e in enumerate(entries)]
    tree = ChunkTree(chunks)
    # Global index over whole files for the no_chunking variant (same specification).
    file_chunks = [Chunk(str(f['file_id']), (min(f['lon']), min(f['lat'])), (max(f['lon']), max(f['lat'])),
                         (min(f['lon']), min(f['lat']))) for f in meta['files']]
    file_tree = ChunkTree(file_chunks)
    (ROOT/'global_index_cost.json').write_text(json.dumps({'construction_seconds':time.perf_counter()-index_started,
        'chunks':len(chunks),'file_units':len(file_chunks),
        'implementation_sha256':hashlib.sha256((ROOT/'chunk_tree.py').read_bytes()).hexdigest()}))

    pin = {'dataset': DATASET, 'files': {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
                     for name in ('ResidentMGSI.java', 'resident-mgsi.jar', 'run_ablation.py', 'physical_index.with_blocks.json',
                                  'golden_results.json', 'golden_extra.json')},
           'scope': meta['scope'], 'cache_per_jvm_bytes': 512*1024**2,
           'global_index_implementation': 'P17/P18 adopted centroid/subtree-bound specification',
           'variants': [{'name': name, 'selection': select, 'executor_flags': flags} for name, select, flags in VARIANTS],
           'layer_mapping': {'global_index': 'driver-side KD-tree entry selection (linear bbox predicate when removed)',
                             'local_index': 'per-entry KD tree bounds on the executor (coordinate scan when removed)',
                             'in_memory': 'batched prefetch and per-JVM plane cache (per-block direct reads when removed)',
                             'logical_chunking': 'tile-granular entries and block-granular IO (whole-file units when removed)'},
           'protocol': 'Four workers; 16 JVMs x 3 subtasks; 48 cores; 8 GiB process configuration per JVM; resident engine; '
                       'one warmup plus five measured queries per variant x case; arm "cleared" clears the application plane '
                       'cache before every query (main-table protocol); arm "retained" clears only at the warmup of each '
                       'variant x case and measured repeats reuse retained planes; variants remove layers from the query path '
                       'only, prepared metadata at initialization is identical and separate; no persisted aggregate results; '
                       'OS/HDFS caches uncontrolled and disclosed.'}
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
        assert all(r['prepared_header_bytes_per_jvm']==8*CONFIG['files'] and r['prepared_spatial_tree_templates_per_jvm']==expected_templates for r in initial_rows)
        ready = time.perf_counter()
        initialization = {'job_id': job_id, 'until_all_48_return_paths_ready_seconds': ready-started,
                          'workers': connections.workers, 'header_bytes_read_in_initialization':16*8*CONFIG['files'],
                          'spatial_templates_prepared_per_jvm':expected_templates, 'physical_index_preparation_seconds_separate': meta['preparation_seconds'],
                          'protocol': pin['protocol']}
        (ROOT/'initialization.json').write_text(json.dumps(initialization, indent=2))
        owner_start=time.perf_counter()
        assignment = locality_assignment(meta, connections)
        files_assignment = file_assignment(meta, connections)
        initialization['ownership_preparation_seconds']=time.perf_counter()-owner_start
        initialization['until_query_service_ready_seconds']=time.perf_counter()-started
        (ROOT/'initialization.json').write_text(json.dumps(initialization,indent=2))
        print('RESIDENT_READY ' + json.dumps({'job_id':job_id, 'initialization_seconds':ready-started, 'workers':48}), flush=True)

        def predicate(e, box):
            return e['lat_max']>=box['lat_min'] and e['lat_min']<=box['lat_max'] and e['lon_max']>=box['lon_min'] and e['lon_min']<=box['lon_max']

        def file_predicate(f, box):
            return max(f['lat'])>=box['lat_min'] and min(f['lat'])<=box['lat_max'] and max(f['lon'])>=box['lon_min'] and min(f['lon'])<=box['lon_max']

        for arm, arm_cases in ARMS:
            for name, select, flags in VARIANTS:
                for case_name, box in cases:
                    if arm_cases is not None and case_name not in arm_cases:
                        continue
                    expected = golden[golden_case.get(case_name, CONFIG['prefix']+'_spatial_'+case_name)]
                    for repeat in range(REPEATS):
                        query_id = '{}_{}_{}_{}'.format(arm, name, case_name, repeat)
                        timer = time.perf_counter()
                        if select == 'file':
                            selected_files = [int(i) for i in file_tree.query(
                                (box['lon_min'],box['lat_min']), (box['lon_max'],box['lat_max']))]
                            assert set(selected_files) == {f['file_id'] for f in meta['files'] if file_predicate(f, box)}
                            byroute = [[] for _ in range(48)]
                            for file_id in selected_files:
                                byroute[files_assignment[file_id]].append(file_id)
                        else:
                            if select == 'tree':
                                selected = [int(i) for i in tree.query(
                                    (box['lon_min'],box['lat_min']), (box['lon_max'],box['lat_max']))]
                            else:
                                selected = [i for i,e in enumerate(entries) if predicate(e, box)]
                            assert set(selected) == {i for i,e in enumerate(entries) if predicate(e, box)}
                            byroute = [[] for _ in range(48)]
                            for entry_id in selected:
                                byroute[assignment[entry_id]].append(entry_id)
                        # Arm "cleared": fresh epoch each query. Arm "retained": one epoch per
                        # variant x case (set at the warmup); measured repeats reuse the planes.
                        epoch = query_id if arm == 'cleared' else '{}_{}_{}_warm'.format(arm, name, case_name)
                        requests = [dict({'type':'query','route':route,'query_id':query_id,'entries':byroute[route],
                                  'box':[box['lat_min'],box['lat_max'],box['lon_min'],box['lon_max']],
                                  'clear_epoch':epoch}, **flags) for route in range(48)]
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
                        assert monthly.keys() == expected.keys(), (query_id, sorted(monthly)[:3], sorted(expected)[:3])
                        for month in monthly:
                            assert monthly[month]['count'] == expected[month]['count'], (query_id,month,'count',monthly[month],expected[month])
                            assert math.isclose(monthly[month]['sum'], expected[month]['sum'], rel_tol=1e-9, abs_tol=1e-6), (query_id,month,'sum',monthly[month],expected[month])
                        record = {'status':'PASS','query_id':query_id,'job_id':job_id,'arm':arm,'variant':name,'case':case_name,
                                  'repeat':repeat, 'mode':'warmup' if repeat==0 else 'measured', 'measured_repeat':repeat,
                                  'cross_query_application_data_cache':'cleared_before_every_query' if arm=='cleared' else 'retained_across_repeats',
                                  'until_final_120_month_results_seconds':elapsed,'monthly_values':monthly,
                                  'physical_read_bytes':sum(row['physical_read_bytes'] for row in rows),
                                  'physical_read_requests':sum(row['physical_read_requests'] for row in rows),
                                  'plane_cache_hits':sum(row['plane_cache_hits'] for row in rows),
                                  'plane_cache_misses':sum(row['plane_cache_misses'] for row in rows),
                                  'entry_mode':rows[0]['entry_mode'],'bounds_mode':rows[0]['bounds_mode'],'cache_mode':rows[0]['cache_mode'],
                                  'active_data_hosts':sorted({row['host'] for row in rows if row['monthly']}),
                                  'worker_rows':rows,'protocol':pin['protocol']}
                        with (ROOT/'results.jsonl').open('a') as stream:
                            stream.write(json.dumps(record,allow_nan=False)+'\n')
                        print('ABLATION_RESULT '+json.dumps({key:value for key,value in record.items() if key not in ('monthly_values','worker_rows','protocol')}), flush=True)
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
            subprocess.run(['/opt/mgsi-cluster/flink','cancel','-m','192.0.2.64:8081',job_id],check=True)
            job=get('/jobs/'+job_id)
        for name,path in [('job','/jobs/'+job_id),('plan','/jobs/'+job_id+'/plan'),('exceptions','/jobs/'+job_id+'/exceptions'),('environment_final','/overview')]:
            (ROOT/(name+'.json')).write_text(json.dumps(get(path),indent=2))
        state['job_state']=job['state']
        (ROOT/'state.json').write_text(json.dumps(state, indent=2))
        connections.server.close()
    assert state['status']=='COMPLETE' and state['job_state']=='FINISHED',state
    print('ABLATION_COMPLETE '+json.dumps(state),flush=True)


if __name__ == '__main__':
    import signal
    def interrupted(signum,frame): raise InterruptedError('Resource supervisor stopped this query job')
    signal.signal(signal.SIGTERM,interrupted)
    main()
