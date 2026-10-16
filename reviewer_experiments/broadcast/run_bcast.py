"""Broadcast-cost experiment (P78): distributed-cache distribution + per-JVM load.

For each dataset meta and each delivery mode:
  prestaged — meta pre-distributed to all nodes at one path (baseline)
  cached    — meta registered as a Flink distributed-cache file; the
              JobManager ships it to every TaskManager at task deploy (the
              broadcast mechanism the paper describes)
submit the resident job, wait for all 48 routes, run the init barrier, collect
per-JVM pool-load milliseconds and heap deltas (reported by the instrumented
engine), then stop. Records submit->ready seconds, meta bytes, and per-JVM
load metrics into bcast_report.json.
"""
import json
import re
import socket
import subprocess
import threading
import time
import queue
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
REST = 'http://192.0.2.64:8081'
PORT = 38950
PORT_COUNTER = [38950]
DATASETS = ['gleam', 'merra2', 'ncep', 'era5', 'chirps']


def next_port():
    PORT_COUNTER[0] += 1
    return PORT_COUNTER[0]


def get(path):
    with urllib.request.urlopen(REST + path, timeout=20) as stream:
        return json.load(stream)


class Connections:
    def __init__(self, port, routes=48):
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
                    self.workers[hello['route']] = hello
                    self.events.put(hello)
                    for line in stream:
                        self.events.put(json.loads(line))
        except Exception as exc:
            self.events.put({'type': 'connection_error', 'error': repr(exc)})

    def send(self, requests):
        self.source.write(''.join(json.dumps(r, separators=(',', ':')) + '\n' for r in requests))
        self.source.flush()

    def results(self, query_id, job_id=None):
        rows = {}
        deadline = time.monotonic() + 600
        last_poll = 0.0
        while len(rows) < self.routes:
            try:
                event = self.events.get(timeout=1)
            except queue.Empty:
                event = None
            if event is not None:
                if event['type'] in ('source', 'worker_ready'):
                    continue
                if event['type'] == 'connection_error':
                    raise RuntimeError('worker connection lost: %r' % event.get('error'))
                assert event['type'] == 'result', event
                assert event['query_id'] == query_id, event
                rows[event['route']] = event
            if job_id and time.monotonic() - last_poll >= 2:
                last_poll = time.monotonic()
                if get('/jobs/'+job_id)['state'] == 'FAILED':
                    root = get('/jobs/'+job_id+'/exceptions').get('root-exception', '')
                    raise RuntimeError('job FAILED while awaiting results: %s' % root[:400])
            if time.monotonic() > deadline:
                raise TimeoutError('only %d/%d routes reported' % (len(rows), self.routes))
        return [rows[i] for i in range(self.routes)]


def one_cell(dataset, mode):
    meta_path = ROOT/('meta_%s.json' % dataset)
    jar = ROOT/('resident-mgsi.jar' if mode == 'prestaged' else 'resident-mgsi-%s-embedded.jar' % dataset)
    port = next_port()
    connections = Connections(port)
    cmd = ['/opt/mgsi-cluster/flink/bin/flink', 'run', '-d', '-m', '192.0.2.64:8081', '-c', 'ResidentMGSI',
           str(jar), '192.0.2.64', str(port), str(meta_path), '48',
           '%s_%s' % (dataset, mode)] + (['cached'] if mode == 'cached' else [])
    t0 = time.perf_counter()
    submitted = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    match = re.search(r'JobID\s+([0-9a-f]{32})', submitted.stdout)
    assert match, submitted.stdout[-2000:]
    job_id = match.group(1)
    deadline = time.monotonic() + 300
    while len(connections.workers) < 48 or connections.source is None:
        assert time.monotonic() < deadline, (len(connections.workers), get('/jobs/'+job_id))
        time.sleep(0.3)
    ready_seconds = time.perf_counter()-t0
    connections.send([{'type': 'query', 'route': r, 'query_id': 'init', 'entries': [], 'box': [-45, 0, -90, 90]}
                      for r in range(48)])
    rows = connections.results('init', job_id)
    jvm_metrics = {}
    for r in rows:
        key = (r['host'], connections.workers[r['route']]['jvm'])
        jvm_metrics[key] = {'pool_load_ms': r['pool_load_ms'],
                            'heap_used_mb_before': r['pool_heap_used_mb_before'],
                            'heap_used_mb_after': r['pool_heap_used_mb_after']}
    try:
        connections.send([{'type': 'stop'}])
    except Exception:
        pass
    deadline = time.monotonic()+60
    while get('/jobs/'+job_id)['state'] not in ('FINISHED', 'FAILED', 'CANCELED') and time.monotonic() < deadline:
        time.sleep(0.5)
    state = get('/jobs/'+job_id)['state']
    if state not in ('FINISHED', 'CANCELED'):
        subprocess.run(['/opt/mgsi-cluster/flink/bin/flink', 'cancel', '-m', '192.0.2.64:8081', job_id],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    connections.server.close()
    time.sleep(2)
    return {'dataset': dataset, 'mode': mode, 'meta_bytes': meta_path.stat().st_size,
            'submit_to_48ready_seconds': ready_seconds, 'job_state': state,
            'jvms_reporting': len(jvm_metrics),
            'pool_load_ms': {('%s|%s' % k): v for k, v in sorted(jvm_metrics.items())}}


def main():
    assert get('/overview')['jobs-running'] == 0, 'cluster busy'
    report = []
    for dataset in DATASETS:
        for mode in ('prestaged', 'cached'):
            cell = one_cell(dataset, mode)
            report.append(cell)
            print('BCAST_CELL '+json.dumps({k: v for k, v in cell.items() if k != 'pool_load_ms'}), flush=True)
            (ROOT/'bcast_report.json').write_text(json.dumps(report, indent=2))
    print('BCAST_COMPLETE', flush=True)


if __name__ == '__main__':
    main()
