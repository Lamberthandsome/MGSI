"""Local stand-in for the resident Flink executor (smoke tests only).

Implements the same socket protocol as ResidentMGSI: one 'source' connection
carrying request lines, N 'worker_ready' connections returning per-route
results. Work is simulated as a delay proportional to the entry count, so the
driver's load sampling, decisions and handoffs can be exercised end to end
without Flink. Results carry dummy monthly aggregates; the driver skips
golden checks in this mode.
"""
import json
import os
import random
import socket
import sys
import threading
import time

HOSTS = ['10.0.0.1', '10.0.0.2', '10.0.0.3', '10.0.0.4']
PER_ENTRY_SECONDS = float(os.environ.get('FAKE_PER_ENTRY', '0.004'))
BASE_SECONDS = float(os.environ.get('FAKE_BASE', '0.01'))
DEBUG = bool(os.environ.get('FAKE_DEBUG'))


def connect(host, port):
    sock = socket.create_connection((host, port))
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    return sock


def main():
    host, port, routes = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    print(f'FAKE_START host={host} port={port} routes={routes}', flush=True)
    per_node = max(1, routes // len(HOSTS))
    writers = {}
    locks = {}
    for route in range(routes):
        node = min(route // per_node, len(HOSTS) - 1)
        sock = connect(host, port)
        hello = {'type': 'worker_ready', 'route': route, 'host': HOSTS[node],
                 'jvm': f'fake-{node}-{route % per_node}'}
        sock.sendall((json.dumps(hello) + '\n').encode())
        writers[route] = sock.makefile('w', encoding='utf-8', buffering=1)
        locks[route] = threading.Lock()
        if route % 12 == 0:
            print(f'FAKE_WORKERS {route + 1}/{routes}', flush=True)
    print('FAKE_WORKERS_DONE', flush=True)

    source = connect(host, port)
    source.sendall(b'{"type":"source"}\n')
    rng = random.Random(11)

    def execute(route, request):
        entries = request.get('entries', [])
        started = time.perf_counter()
        time.sleep(BASE_SECONDS + len(entries) * PER_ENTRY_SECONDS * (0.8 + 0.4 * rng.random()))
        result = {'type': 'result', 'query_id': request['query_id'], 'route': route,
                  'host': HOSTS[min(route // per_node, len(HOSTS) - 1)],
                  'worker_seconds': time.perf_counter() - started,
                  'physical_read_bytes': len(entries) * 4096, 'physical_read_requests': len(entries),
                  'plane_cache_hits': 0, 'plane_cache_misses': len(entries),
                  'monthly': {'1981-01': {'sum': float(len(entries)), 'count': len(entries)}}}
        with locks[route]:
            writers[route].write(json.dumps(result, separators=(',', ':')) + '\n')
            writers[route].flush()
        if DEBUG:
            print(f"FAKE_RESULT route={route} qid={request['query_id']}", flush=True)

    with source.makefile('r', encoding='utf-8') as stream:
        for line in stream:
            request = json.loads(line)
            if request.get('type') == 'stop':
                break
            if DEBUG:
                print(f"FAKE_REQ route={request['route']} qid={request['query_id']}", flush=True)
            threading.Thread(target=execute, args=(request['route'], request), daemon=True).start()
    time.sleep(0.5)


if __name__ == '__main__':
    main()
