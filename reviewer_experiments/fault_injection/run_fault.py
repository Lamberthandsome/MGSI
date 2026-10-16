"""Fault-injection driver (P67): TaskManager hard-kill during a steady query
stream, then measured recovery on the resident engine.

Phases:
  1. Submit the resident job (48 routes, standard 16-TM layout).
  2. Steady phase: sequential golden-checked queries (yangtze/R5/global).
  3. Kill one TaskManager JVM with SIGKILL mid-stream; record which queries
     were in flight, how the stream failed, and the kill-to-job-FAILED time.
  4. Recovery: restart the unit, resubmit the job, measure resubmit-to-ready
     (metadata reload included), first-query (cold) vs steady mean, and verify
     the recovered system passes the same golden checks.

Honest scope: the engine has no in-job failover (restart strategy: noRestart);
the actual recovery path is TaskManager restart + job resubmission + metadata
reload + cache re-warm. That path is what this experiment measures.
"""
import argparse
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

from chunk_tree import Chunk, ChunkTree

ROOT = Path(__file__).resolve().parent
REST = 'http://192.0.2.64:8081'
YANGTZE = {'lat_min': 27.0, 'lat_max': 34.0, 'lon_min': 102.0, 'lon_max': 112.0}
KILL_WORKER = '192.0.2.66'
KILL_UNIT = 'mgsi-flink-taskmanager@3'


def get(path):
    with urllib.request.urlopen(REST + path, timeout=20) as stream:
        return json.load(stream)


class Connections:
    def __init__(self, port, routes):
        self.routes = routes
        self.server = socket.socket()
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind(('192.0.2.64', port))
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
            self.events.put({'type': 'connection_error', 'error': repr(exc)})

    def send(self, requests):
        self.source.write(''.join(json.dumps(request, separators=(',', ':')) + '\n' for request in requests))
        self.source.flush()

    def try_results(self, query_id, timeout):
        """Collect up to routes rows; return (rows, complete)."""
        rows = {}
        deadline = time.monotonic() + timeout
        while len(rows) < self.routes and time.monotonic() < deadline:
            try:
                event = self.events.get(timeout=1)
            except queue.Empty:
                continue
            if event['type'] in ('source', 'worker_ready'):
                continue
            if event['type'] == 'connection_error':
                self.events.put(event)
                break
            assert event['type'] == 'result', event
            if event['query_id'] != query_id:
                continue
            rows[event['route']] = event
        return rows, len(rows) == self.routes

    def results(self, query_id, timeout=1800):
        rows = {}
        deadline = time.monotonic() + timeout
        while len(rows) < self.routes:
            event = self.events.get(timeout=max(1, deadline-time.monotonic()))
            if event['type'] in ('source', 'worker_ready'):
                continue
            if event['type'] == 'connection_error':
                raise ConnectionError(event)
            assert event['type'] == 'result', event
            assert event['query_id'] == query_id, event
            assert event['route'] not in rows, event
            rows[event['route']] = event
        return [rows[i] for i in range(self.routes)]


def locality_assignment(meta, connections):
    groups = defaultdict(list)
    for route, info in connections.workers.items():
        groups[(info['host'], info['jvm'])].append(route)
    assert len(groups) == 16 and all(len(v) == 3 for v in groups.values())
    spatial = defaultdict(list)
    for entry in meta['entries']:
        spatial[(entry['file_id'], entry['rows'][0]//450, entry['cols'][0]//720)].append(entry)
    physical = [{int(r[0]): r for r in f['physical_chunks']} for f in meta['files']]
    load = {g: 0.0 for g in groups}
    assignment = {}
    for group_key, entries in sorted(spatial.items()):
        f = meta['files'][group_key[0]]
        pc, pg = f['physical_shape'], f['physical_grid']
        r0 = min(e['rows'][0] for e in entries); r1 = max(e['rows'][1] for e in entries)
        c0 = min(e['cols'][0] for e in entries); c1 = max(e['cols'][1] for e in entries)
        weights = defaultdict(float); cost = 0; keys = set()
        for ti in f['time_indices']:
            for rc in range(r0//pc[1], (r1-1)//pc[1]+1):
                for cc in range(c0//pc[2], (c1-1)//pc[2]+1):
                    key = (ti//pc[0]*pg[1]+rc)*pg[2]+cc
                    if key in keys:
                        continue
                    keys.add(key)
                    _, offset, size, mask = physical[f['file_id']][key]
                    cost += size
                    for block in f['hdfs_blocks']:
                        overlap = max(0, min(offset+size, block['offset']+block['length'])-max(offset, block['offset']))
                        if overlap:
                            for host in block['hosts']:
                                weights[host] += overlap
        maximum = max(weights.values()) if weights else 0
        choices = [g for g in groups if not weights or weights[g[0]] >= maximum*0.70] or list(groups)
        owner = min(choices, key=lambda g: (load[g], g))
        load[owner] += cost
        for entry in entries:
            assignment[entry['entry_id']] = min(groups[owner], key=lambda r: r)
    return assignment


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=38850)
    parser.add_argument('--steady-queries', type=int, default=6)
    args = parser.parse_args()
    n = 48
    report = {'phases': {}}
    meta_path = ROOT / 'physical_index.with_blocks.json'
    meta = json.loads(meta_path.read_text())
    golden = {row['case']: row['monthly_values'] for row in json.loads((ROOT/'golden_results.json').read_text())}
    golden.update({row['case']: row['monthly_values'] for row in json.loads((ROOT/'golden_extra.json').read_text())})
    entries = meta['entries']
    lats = meta['files'][0]['lat']; lons = meta['files'][0]['lon']
    boxes = {'temporal_yangtze': YANGTZE,
             'temporal_global': {'lat_min': min(lats), 'lat_max': max(lats), 'lon_min': min(lons), 'lon_max': max(lons)},
             'R5': meta['boxes']['5']}
    golden_case = {'temporal_yangtze': 'gleam_1981_1990_temporal_yangtze',
                   'temporal_global': 'gleam_1981_1990_temporal_global',
                   'R5': 'gleam_1981_1990_spatial_R5'}
    chunks = [Chunk(str(i), (e['lon_min'], e['lat_min']), (e['lon_max'], e['lat_max']),
                    (e['lon_min'], e['lat_min'])) for i, e in enumerate(entries)]
    tree = ChunkTree(chunks)
    pin = {'files': {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
                     ('ResidentMGSI.java', 'resident-mgsi.jar', 'run_fault.py', 'physical_index.with_blocks.json')},
           'kill': {'worker': KILL_WORKER, 'unit': KILL_UNIT, 'signal': 'SIGKILL'}}
    (ROOT/'run_pin.json').write_text(json.dumps(pin, indent=2))
    report['pin'] = pin

    def submit_job(port):
        started = time.perf_counter()
        submitted = subprocess.run(['/opt/mgsi-cluster/flink/bin/flink', 'run', '-d', '-m', '192.0.2.64:8081', '-c', 'ResidentMGSI',
                                    str(ROOT/'resident-mgsi.jar'), '192.0.2.64', str(port), str(meta_path), '48', 'fault'],
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
        match = re.search(r'JobID\s+([0-9a-f]{32})', submitted.stdout)
        assert match, submitted.stdout
        return match.group(1), time.perf_counter()-started

    def wait_ready(connections, job_id):
        deadline = time.monotonic()+300
        while len(connections.workers) < n or connections.source is None:
            assert time.monotonic() < deadline, (len(connections.workers), get('/jobs/'+job_id))
            assert get('/jobs/'+job_id)['state'] not in ('FAILED', 'CANCELED', 'FINISHED')
            time.sleep(0.5)

    def one_query(connections, assignment, query_id, case_name, box, timeout=1800):
        timer = time.perf_counter()
        selected = [int(i) for i in tree.query((box['lon_min'], box['lat_min']), (box['lon_max'], box['lat_max']))]
        byroute = [[] for _ in range(n)]
        for entry_id in selected:
            byroute[assignment[entry_id]].append(entry_id)
        connections.send([{'type': 'query', 'route': route, 'query_id': query_id, 'entries': byroute[route],
                           'box': [box['lat_min'], box['lat_max'], box['lon_min'], box['lon_max']],
                           'clear_epoch': query_id} for route in range(n)])
        rows = connections.results(query_id, timeout)
        monthly = {}
        for row in rows:
            for month, value in row['monthly'].items():
                bucket = monthly.setdefault(month, {'sum': 0.0, 'count': 0})
                bucket['sum'] += value['sum']
                bucket['count'] += value['count']
        elapsed = time.perf_counter()-timer
        expected = golden[golden_case[case_name]]
        assert monthly.keys() == expected.keys()
        for month in monthly:
            assert monthly[month]['count'] == expected[month]['count'], (query_id, month, 'count')
            assert math.isclose(monthly[month]['sum'], expected[month]['sum'], rel_tol=1e-9, abs_tol=1e-6), (query_id, month, 'sum')
        return elapsed

    case_cycle = ['temporal_yangtze', 'R5', 'temporal_global']

    connections = Connections(args.port, n)
    job_id, submit_seconds = submit_job(args.port)
    wait_ready(connections, job_id)
    connections.send([{'type': 'query', 'route': r, 'query_id': 'init', 'entries': [], 'box': [-45, 0, -90, 90]} for r in range(n)])
    connections.results('init')
    assignment = locality_assignment(meta, connections)
    report['phases']['initial_submit'] = {'job_id': job_id, 'submit_seconds': submit_seconds}
    print('FAULT_READY '+job_id, flush=True)

    steady = []
    for i in range(args.steady_queries):
        case_name = case_cycle[i % len(case_cycle)]
        elapsed = one_query(connections, assignment, 'steady_{}'.format(i), case_name, boxes[case_name])
        steady.append({'case': case_name, 'seconds': elapsed, 'status': 'PASS'})
        print('STEADY {} {:.2f}s'.format(case_name, elapsed), flush=True)
    steady_mean = sum(s['seconds'] for s in steady)/len(steady)
    report['phases']['steady'] = {'queries': steady, 'mean_seconds': steady_mean}
    (ROOT/'fault_report.json').write_text(json.dumps(report, indent=2))

    # --- fault injection ---
    killed_at = time.monotonic()
    subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', 'root@'+KILL_WORKER,
                    'systemctl kill -s SIGKILL {}'.format(KILL_UNIT)], check=True)
    print('KILLED {} at {}'.format(KILL_UNIT, time.ctime()), flush=True)
    case_name = 'R5'
    failure = {'killed_query': case_name}
    try:
        one_query(connections, assignment, 'during_fault', case_name, boxes[case_name], timeout=90)
        failure['outcome'] = 'unexpectedly_completed'
    except (ConnectionError, AssertionError, queue.Empty) as exc:
        failure['outcome'] = type(exc).__name__
        failure['detail'] = str(exc)[:300]
    failed_state = None
    while time.monotonic()-killed_at < 300:
        state = get('/jobs/'+job_id)['state']
        if state in ('FAILED', 'CANCELED'):
            failed_state = state
            break
        time.sleep(0.5)
    failure['kill_to_job_failed_seconds'] = time.monotonic()-killed_at if failed_state else None
    failure['job_state_after'] = failed_state or get('/jobs/'+job_id)['state']
    report['phases']['fault'] = failure
    (ROOT/'fault_report.json').write_text(json.dumps(report, indent=2))
    print('FAULT_OBSERVED '+json.dumps(failure), flush=True)
    try:
        connections.send([{'type': 'stop'}])
    except Exception:
        pass

    # --- recovery ---
    recover_started = time.monotonic()
    subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', 'root@'+KILL_WORKER,
                    'systemctl start {}'.format(KILL_UNIT)], check=True)
    tm_back = None
    while time.monotonic()-recover_started < 180:
        tms = get('/taskmanagers')['taskmanagers']
        if sum(1 for t in tms) >= 16:
            tm_back = time.monotonic()-recover_started
            break
        time.sleep(1)
    subprocess.run(['/opt/mgsi-cluster/flink/bin/flink', 'cancel', '-m', '192.0.2.64:8081', job_id],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)
    connections2 = Connections(args.port+1, n)
    job_id2, submit2_seconds = submit_job(args.port+1)
    ready_t0 = time.monotonic()
    wait_ready(connections2, job_id2)
    resubmit_to_ready = time.monotonic()-ready_t0
    connections2.send([{'type': 'query', 'route': r, 'query_id': 'init2', 'entries': [], 'box': [-45, 0, -90, 90]} for r in range(n)])
    connections2.results('init2')
    assignment2 = locality_assignment(meta, connections2)
    first_case = 'temporal_yangtze'
    first_elapsed = one_query(connections2, assignment2, 'recovered_first', first_case, boxes[first_case])
    recovered = []
    for i, case_name in enumerate(case_cycle):
        elapsed = one_query(connections2, assignment2, 'recovered_{}'.format(i), case_name, boxes[case_name])
        recovered.append({'case': case_name, 'seconds': elapsed, 'status': 'PASS'})
    recovery = {'tm_restart_seconds': tm_back, 'resubmit_seconds': submit2_seconds,
                'resubmit_to_workers_ready_seconds': resubmit_to_ready,
                'first_query_cold_seconds': first_elapsed,
                'steady_mean_seconds_for_compare': steady_mean,
                'queries_after_recovery': recovered,
                'all_golden_pass': True}
    report['phases']['recovery'] = recovery
    (ROOT/'fault_report.json').write_text(json.dumps(report, indent=2))
    print('RECOVERY_MEASURED '+json.dumps({k: v for k, v in recovery.items() if k != 'queries_after_recovery'}), flush=True)

    connections2.send([{'type': 'stop'}])
    deadline = time.monotonic()+60
    while get('/jobs/'+job_id2)['state'] not in ('FINISHED', 'FAILED', 'CANCELED') and time.monotonic() < deadline:
        time.sleep(0.5)
    report['final_job_state'] = get('/jobs/'+job_id2)['state']
    (ROOT/'fault_report.json').write_text(json.dumps(report, indent=2))
    print('FAULT_COMPLETE '+json.dumps(report['final_job_state']), flush=True)


if __name__ == '__main__':
    main()
