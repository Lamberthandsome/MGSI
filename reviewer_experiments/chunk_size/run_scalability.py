"""Scalability driver (P57(b)): latency + closed-loop throughput at N TaskManagers.

Usage: python run_scalability.py --tms N --label tmN [--meta physical_index.with_blocks.json]
       [--golden-extra golden_extra.json] [--throughput-seconds 120] [--port 38800+N]

Latency phase: 1 warmup + 5 measured for temporal_yangtze, R5, temporal_global
against golden monthly sums/counts (rel_tol 1e-9); cache cleared per query.
Throughput phase: pipelined closed loop, 8 in-flight queries round-robin over the
same cases, unique clear_epoch per query, every result golden-checked.
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
CASES_WANTED = ('temporal_yangtze', 'R5', 'temporal_global')


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
        self.stash = defaultdict(list)
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
        for event in self.stash.pop(query_id, []):
            rows[event['route']] = event
        deadline = time.monotonic() + 1800
        while len(rows) < self.routes:
            event = self.events.get(timeout=max(1, deadline-time.monotonic()))
            if event['type'] in ('source', 'worker_ready'):
                continue
            assert event['type'] == 'result', event
            if event['query_id'] != query_id:
                self.stash[event['query_id']].append(event)
                continue
            assert event['route'] not in rows, event
            rows[event['route']] = event
        return [rows[i] for i in range(self.routes)]


def locality_assignment(meta, connections, tile_rows, tile_cols):
    groups = defaultdict(list)
    for route, info in connections.workers.items():
        groups[(info['host'], info['jvm'])].append(route)
    # Flink's slot allocator may co-locate several subtasks on one TaskManager;
    # record the actual placement instead of assuming one subtask per TM.
    placement = {'jvms_used': len(groups), 'tms_available': len(connections.workers),
                 'routes_per_jvm': sorted(len(v) for v in groups.values()),
                 'detail': {'{}:{}'.format(k[0], k[1]): v for k, v in sorted(groups.items())}}
    (ROOT/('placement_%s.json' % LABEL)).write_text(json.dumps(placement, indent=2))
    print('PLACEMENT '+json.dumps(placement), flush=True)
    spatial = defaultdict(list)
    for entry in meta['entries']:
        spatial[(entry['file_id'], entry['rows'][0]//tile_rows, entry['cols'][0]//tile_cols)].append(entry)
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
    (ROOT/'locality_assignment_{}.json'.format(LABEL)).write_text(json.dumps(
        [{'entry_id': k, 'route': v} for k, v in sorted(assignment.items())], indent=2))
    return assignment


LABEL = 'run'


def verify(monthly, expected, query_id):
    assert monthly.keys() == expected.keys(), (query_id, sorted(monthly)[:3], sorted(expected)[:3])
    for month in monthly:
        assert monthly[month]['count'] == expected[month]['count'], (query_id, month, 'count', monthly[month], expected[month])
        assert math.isclose(monthly[month]['sum'], expected[month]['sum'], rel_tol=1e-9, abs_tol=1e-6), (query_id, month, 'sum', monthly[month], expected[month])


def main():
    global LABEL
    parser = argparse.ArgumentParser()
    parser.add_argument('--tms', type=int, required=True)
    parser.add_argument('--slots-per-tm', type=int, default=1)
    parser.add_argument('--label', required=True)
    parser.add_argument('--meta', default='physical_index.with_blocks.json')
    parser.add_argument('--golden-extra', default='golden_extra.json')
    parser.add_argument('--throughput-seconds', type=int, default=120)
    parser.add_argument('--port', type=int, default=None)
    args = parser.parse_args()
    LABEL = args.label
    n = args.tms * args.slots_per_tm
    port = args.port if args.port is not None else 38800 + args.tms

    overview = get('/overview')
    assert overview['jobs-running'] == 0, overview
    assert overview['taskmanagers'] == args.tms and overview['slots-available'] == n, \
        'expected %d TaskManagers with %d slots: %s' % (args.tms, n, overview)
    assert not (ROOT/('results_%s.jsonl' % LABEL)).exists(), 'previous records retained'
    meta_path = ROOT / args.meta
    meta = json.loads(meta_path.read_text())
    golden = {row['case']: row['monthly_values'] for row in json.loads((ROOT/'golden_results.json').read_text())}
    if (ROOT/args.golden_extra).exists():
        golden.update({row['case']: row['monthly_values'] for row in json.loads((ROOT/args.golden_extra).read_text())})
    entries = meta['entries']

    lats = meta['files'][0]['lat']; lons = meta['files'][0]['lon']
    global_box = {'lat_min': min(lats), 'lat_max': max(lats), 'lon_min': min(lons), 'lon_max': max(lons)}
    boxes = {'temporal_yangtze': YANGTZE, 'temporal_global': global_box,
             'R5': meta['boxes']['5']}
    info_path = ROOT/('slice_info_%s.json' % LABEL)
    if info_path.exists():
        info = json.loads(info_path.read_text())
        prefix = info['golden_prefix']
        tile_rows, tile_cols = info.get('tile_rows', 450), info.get('tile_cols', 720)
    else:
        prefix = 'gleam_1981_1990'
        tile_rows, tile_cols = 450, 720
    golden_case = {k: prefix+v for k, v in
                   (('temporal_yangtze', '_temporal_yangtze'), ('temporal_global', '_temporal_global'), ('R5', '_spatial_R5'))}

    index_started = time.perf_counter()
    chunks = [Chunk(str(i), (e['lon_min'], e['lat_min']), (e['lon_max'], e['lat_max']), (e['lon_min'], e['lat_min'])) for i, e in enumerate(entries)]
    tree = ChunkTree(chunks)
    (ROOT/('global_index_cost_%s.json' % LABEL)).write_text(json.dumps(
        {'construction_seconds': time.perf_counter()-index_started, 'chunks': len(chunks)}))

    protocol = ('GLEAM Eb; {} one-slot TaskManagers (3 cores, 8 GiB each), one subtask per TaskManager '
                '(tm1 measured before the one-slot change at parallelism 1, identical for a single subtask); '
                'latency 1 warmup + 5 measured per case with cache cleared before every query; '
                'throughput phase: pipelined closed loop, 8 in-flight, {} s, same cases, cache cleared per query; '
                'every result golden-checked.').format(n, args.throughput_seconds)
    pin = {'label': LABEL, 'tms': n, 'protocol': protocol,
           'files': {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
                     ('ResidentMGSI.java', 'resident-mgsi.jar', 'run_scalability.py', args.meta)}}
    (ROOT/('run_pin_%s.json' % LABEL)).write_text(json.dumps(pin, indent=2))

    connections = Connections(port, n)
    started = time.perf_counter()
    submitted = subprocess.run(['/opt/mgsi-cluster/flink/bin/flink', 'run', '-d', '-m', '192.0.2.64:8081', '-c', 'ResidentMGSI',
                                str(ROOT/'resident-mgsi.jar'), '192.0.2.64', str(port), str(meta_path), str(n), LABEL],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    (ROOT/('submission_%s.log' % LABEL)).write_text(submitted.stdout)
    assert submitted.returncode == 0, submitted.stdout
    match = re.search(r'JobID\s+([0-9a-f]{32})', submitted.stdout)
    assert match, submitted.stdout
    job_id = match.group(1)
    state = {'status': 'INITIALIZING', 'job_id': job_id, 'label': LABEL}
    (ROOT/('state_%s.json' % LABEL)).write_text(json.dumps(state, indent=2))
    out = (ROOT/('results_%s.jsonl' % LABEL)).open('a')

    def run_query(query_id, case_name, box, repeat, mode, phase):
        timer = time.perf_counter()
        selected = [int(i) for i in tree.query((box['lon_min'], box['lat_min']), (box['lon_max'], box['lat_max']))]
        predicate = lambda e: e['lat_max'] >= box['lat_min'] and e['lat_min'] <= box['lat_max'] and e['lon_max'] >= box['lon_min'] and e['lon_min'] <= box['lon_max']
        assert set(selected) == {i for i, e in enumerate(entries) if predicate(e)}
        byroute = [[] for _ in range(n)]
        for entry_id in selected:
            byroute[assignment[entry_id]].append(entry_id)
        requests = [{'type': 'query', 'route': route, 'query_id': query_id, 'entries': byroute[route],
                     'box': [box['lat_min'], box['lat_max'], box['lon_min'], box['lon_max']],
                     'clear_epoch': query_id} for route in range(n)]
        connections.send(requests)
        rows = connections.results(query_id)
        monthly = {}
        for row in rows:
            for month, value in row['monthly'].items():
                bucket = monthly.setdefault(month, {'sum': 0.0, 'count': 0})
                bucket['sum'] += value['sum']
                bucket['count'] += value['count']
        elapsed = time.perf_counter()-timer
        verify(monthly, golden[golden_case[case_name]], query_id)
        record = {'status': 'PASS', 'query_id': query_id, 'job_id': job_id, 'label': LABEL, 'phase': phase,
                  'case': case_name, 'repeat': repeat, 'mode': mode, 'seconds': elapsed,
                  'physical_read_bytes': sum(r['physical_read_bytes'] for r in rows),
                  'plane_cache_hits': sum(r['plane_cache_hits'] for r in rows),
                  'plane_cache_misses': sum(r['plane_cache_misses'] for r in rows)}
        out.write(json.dumps(record, allow_nan=False)+'\n'); out.flush()
        state['last_completed'] = query_id
        (ROOT/('state_%s.json' % LABEL)).write_text(json.dumps(state, indent=2))
        return elapsed

    try:
        deadline = time.monotonic()+300
        while len(connections.workers) < n or connections.source is None:
            assert time.monotonic() < deadline, (len(connections.workers), get('/jobs/'+job_id))
            assert get('/jobs/'+job_id)['state'] not in ('FAILED', 'CANCELED', 'FINISHED')
            time.sleep(0.5)
        connections.send([{'type': 'query', 'route': route, 'query_id': 'init', 'entries': [], 'box': [-45, 0, -90, 90]} for route in range(n)])
        connections.results('init')
        assignment = locality_assignment(meta, connections, tile_rows, tile_cols)
        print('SCAL_READY '+json.dumps({'label': LABEL, 'tms': n, 'init_seconds': time.perf_counter()-started}), flush=True)

        for case_name in CASES_WANTED:
            box = boxes[case_name]
            for repeat in range(6):
                run_query('lat_{}_{}_{}'.format(LABEL, case_name, repeat), case_name, box, repeat,
                          'warmup' if repeat == 0 else 'measured', 'latency')
        print('LATENCY_DONE '+LABEL, flush=True)

        if args.throughput_seconds > 0:
            phase_start = time.monotonic()
            completed = 0
            seq = 0
            latencies = []
            while time.monotonic() - phase_start < args.throughput_seconds:
                batch = []
                for s in range(8):
                    case_name = CASES_WANTED[(seq+s) % len(CASES_WANTED)]
                    batch.append((case_name, boxes[case_name], 'tp_{}_{}'.format(LABEL, seq+s)))
                batch_t0 = time.monotonic()
                for case_name, box, qid in batch:
                    selected = [int(i) for i in tree.query((box['lon_min'], box['lat_min']), (box['lon_max'], box['lat_max']))]
                    byroute = [[] for _ in range(n)]
                    for entry_id in selected:
                        byroute[assignment[entry_id]].append(entry_id)
                    connections.send([{'type': 'query', 'route': route, 'query_id': qid, 'entries': byroute[route],
                                       'box': [box['lat_min'], box['lat_max'], box['lon_min'], box['lon_max']],
                                       'clear_epoch': qid} for route in range(n)])
                batch_t0 = time.monotonic()
                for case_name, box, qid in batch:
                    rows = connections.results(qid)
                    monthly = {}
                    for row in rows:
                        for month, value in row['monthly'].items():
                            bucket = monthly.setdefault(month, {'sum': 0.0, 'count': 0})
                            bucket['sum'] += value['sum']
                            bucket['count'] += value['count']
                    verify(monthly, golden[golden_case[case_name]], qid)
                    completed += 1
                batch_elapsed = time.monotonic()-batch_t0
                latencies.append(batch_elapsed/len(batch))
                seq += len(batch)
            duration = time.monotonic()-phase_start
            record = {'status': 'PASS', 'label': LABEL, 'phase': 'throughput', 'seconds': duration,
                      'completed_queries': completed, 'qps': completed/duration,
                      'in_flight': 8, 'mean_batch_query_seconds': sum(latencies)/len(latencies) if latencies else None}
            out.write(json.dumps(record, allow_nan=False)+'\n'); out.flush()
            print('THROUGHPUT_DONE '+json.dumps(record), flush=True)
        state['status'] = 'COMPLETE'
    finally:
        if connections.source is not None:
            connections.send([{'type': 'stop'}])
        deadline = time.monotonic()+60
        while get('/jobs/'+job_id)['state'] not in ('FINISHED', 'FAILED', 'CANCELED') and time.monotonic() < deadline:
            time.sleep(0.5)
        job = get('/jobs/'+job_id)
        if job['state'] not in ('FINISHED', 'FAILED', 'CANCELED'):
            subprocess.run(['/opt/mgsi-cluster/flink', 'cancel', '-m', '192.0.2.64:8081', job_id], check=True)
        state['job_state'] = get('/jobs/'+job_id)['state']
        (ROOT/('state_%s.json' % LABEL)).write_text(json.dumps(state, indent=2))
        connections.server.close()
        out.close()
    assert state['status'] == 'COMPLETE' and state['job_state'] == 'FINISHED', state
    print('SCAL_COMPLETE '+LABEL, flush=True)


if __name__ == '__main__':
    main()
