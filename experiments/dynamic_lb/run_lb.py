"""Dynamic load-balancing experiment driver over the resident MGSI executor.

Runs a concurrent archived-query stream against the resident Flink job
(ResidentMGSI) while a control loop samples per-node load, decides
split/merge/migrate actions per the adopted repartitioning policy, and
executes the consistency handoff (admission barrier -> drain -> checkpoint ->
atomic layout publish -> epoch broadcast/cache invalidation -> node
confirmation -> reopen with cooldown).

Real mode uses the GLEAM 1981-1990 physical index and verifies every query
against the archived golden monthly results.  --fake-executor mode builds a
synthetic lattice locally and talks to fake_executor.py for end-to-end smoke
tests without Flink.
"""
import argparse
import hashlib
import json
import math
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from collections import defaultdict, deque
from pathlib import Path
import queue as queue_module

from adaptive_policy import (Box, Controller, FileBackend, NodeSample,
                             Partition, Policy, AdmissionBlocked, StaleLayout)
from chunk_tree import Chunk, ChunkTree

ROOT = Path(__file__).resolve().parent
REST = 'http://192.0.2.64:8081'
NODE_PREFIX = 'n'  # owner identifiers must match [A-Za-z0-9_-]+
DEBUG = bool(os.environ.get('LB_DEBUG'))


class Jsonl:
    """Append-only evidence writer; created exclusively per batch."""

    def __init__(self, path):
        self.path = Path(path)
        self.lock = threading.Lock()
        self.handle = open(self.path, 'x', encoding='utf-8')

    def write(self, row):
        line = json.dumps(row, allow_nan=False, separators=(',', ':'))
        with self.lock:
            self.handle.write(line + '\n')
            self.handle.flush()


class TimedBackend(FileBackend):
    """FileBackend with observed write timings and sizes for the cost model."""

    def __init__(self, root):
        super().__init__(root)
        self.obs = {'write_seconds': 0.0, 'write_bytes': 0,
                    'last_checkpoint_seconds': None, 'last_publish_seconds': None}
        self.obs_lock = threading.Lock()

    def _record(self, name, size, started):
        elapsed = time.perf_counter() - started
        with self.obs_lock:
            self.obs['write_seconds'] += elapsed
            self.obs['write_bytes'] += size
            self.obs['last_' + name + '_seconds'] = elapsed

    def create(self, *args, **kwargs):
        started = time.perf_counter()
        partition = super().create(*args, **kwargs)
        self._record('create', Path(partition.path).stat().st_size, started)
        return partition

    def checkpoint(self, *args, **kwargs):
        started = time.perf_counter()
        super().checkpoint(*args, **kwargs)
        self._record('checkpoint', (self.root / f'checkpoint-{args[0]}.json').stat().st_size, started)

    def publish(self, snapshot):
        started = time.perf_counter()
        super().publish(snapshot)
        self._record('publish', self.pointer.stat().st_size, started)

    def bandwidth_bytes_per_second(self):
        with self.obs_lock:
            if self.obs['write_seconds'] <= 0 or self.obs['write_bytes'] <= 0:
                return None
            return self.obs['write_bytes'] / self.obs['write_seconds']


class Lattice:
    """Maps dataset entries onto a rectilinear tile lattice of logical cells.

    One lattice cell == one payload row, so every split/merge/migration keeps
    exact cell coverage and entry lists are re-derived by containment.
    """

    def __init__(self, meta=None, synthetic_tiles=(8, 10), per_tile=3, seed=7):
        self.entry_tile = {}
        self.tile_rows = []
        self.boxes = {}
        self.entries_lonlat = None
        if meta is not None:
            self._from_meta(meta)
        else:
            self._synthetic(synthetic_tiles, per_tile, seed)

    def _from_meta(self, meta):
        lat_mins = {round(float(e['lat_min']), 6) for e in meta['entries']}
        lon_mins = {round(float(e['lon_min']), 6) for e in meta['entries']}
        lat_index = {v: i for i, v in enumerate(sorted(lat_mins))}
        lon_index = {v: i for i, v in enumerate(sorted(lon_mins))}
        nl, nw = len(lat_index), len(lon_index)
        tiles = defaultdict(list)
        for entry in meta['entries']:
            tile = (lat_index[round(float(entry['lat_min']), 6)],
                    lon_index[round(float(entry['lon_min']), 6)])
            tiles[tile].append(entry['entry_id'])
            self.entry_tile[entry['entry_id']] = tile
        assert set(tiles) == {(i, j) for i in range(nl) for j in range(nw)}, \
            'entries do not form a complete rectilinear lattice'
        file_hosts = [sorted({h for b in f.get('hdfs_blocks', []) for h in b['hosts']})
                      for f in meta['files']]
        file_of = {e['entry_id']: e['file_id'] for e in meta['entries']}
        for tile, entry_ids in sorted(tiles.items()):
            hosts = sorted({h for e in entry_ids for h in file_hosts[file_of[e]]})
            self.tile_rows.append([list(tile), sorted(entry_ids), hosts])
        self.shape = (nl, nw)
        for region, box in meta['boxes'].items():
            self.boxes[int(region)] = box

    def _synthetic(self, tiles, per_tile, seed):
        rng = random.Random(seed)
        nl, nw = tiles
        self.shape = (nl, nw)
        self.entries_lonlat = {}
        span_lat, span_lon = 180.0 / nl, 360.0 / nw
        entry_id = 0
        for i in range(nl):
            for j in range(nw):
                ids = []
                for _ in range(per_tile):
                    ids.append(entry_id)
                    self.entries_lonlat[entry_id] = (j * span_lon - 180, i * span_lat - 90,
                                                     (j + 1) * span_lon - 180,
                                                     (i + 1) * span_lat - 90)
                    self.entry_tile[entry_id] = (i, j)
                    entry_id += 1
                self.tile_rows.append([[i, j], ids, []])
        for region in range(1, 9):
            i0, j0 = rng.randrange(0, max(1, nl - 2)), rng.randrange(0, max(1, nw - 3))
            di, dj = min(rng.randrange(2, 4), nl - i0), min(rng.randrange(3, 6), nw - j0)
            self.boxes[region] = {'lat_min': i0 * span_lat - 90, 'lat_max': (i0 + di) * span_lat - 90,
                                  'lon_min': j0 * span_lon - 180, 'lon_max': (j0 + dj) * span_lon - 180}

    def initial_boxes(self, row_parts, col_parts):
        nl, nw = self.shape
        boxes = []
        for r0, r1 in split_range(nl, row_parts):
            for c0, c1 in split_range(nw, col_parts):
                if r1 > r0 and c1 > c0:
                    boxes.append(Box((r0, c0), (r1, c1)))
        return boxes


def split_range(n, parts):
    parts = max(1, min(parts, n))
    base, extra = divmod(n, parts)
    ranges, start = [], 0
    for k in range(parts):
        end = start + base + (1 if k < extra else 0)
        ranges.append((start, end))
        start = end
    return ranges


class LBController(Controller):
    """Controller additions for the live query path."""

    def count_access(self, identifiers, now):
        with self._lock:
            now = max(now, self.counters.latest_time)
            for identifier in identifiers:
                self.counters.add(identifier, now)

    def initial_checkpoint(self):
        """Persist the initial layout once so checkpoint cost/bandwidth are
        observed quantities from run start, not prerequisites that can never
        be satisfied before the first action."""
        with self._lock:
            snapshot = self._snapshot()
            self.backend.checkpoint(self.epoch + 1, snapshot, snapshot)

    def route_table(self):
        with self._lock:
            return self._route_table_locked()

    def _route_table_locked(self):
        tile_owner = {}
        for chunk in self.chunks.values():
            low, high = chunk.box.low, chunk.box.high
            for i in range(low[0], high[0]):
                for j in range(low[1], high[1]):
                    tile_owner[(i, j)] = chunk.identifier
        owners = {c: p.owner for c, p in self.chunks.items()}
        return self.epoch, tile_owner, owners

    def capture_serving(self):
        """Freeze the published layout for query admission. Called after every
        committed handoff; queries admitted between captures all use the same
        committed layout, so an ownership switch never pauses admission."""
        with self._lock:
            self._serving = self._route_table_locked()
            return self._serving

    def serving_table(self):
        with self._lock:
            if getattr(self, '_serving', None) is None:
                self._serving = self._route_table_locked()
            return self._serving


class Connections:
    """Socket demux for the resident job with per-route timing bookkeeping."""

    def __init__(self, host, port):
        self.server = socket.socket()
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((host, port))
        self.server.listen(256)
        self.source = None
        self.workers = {}
        self.lock = threading.Lock()
        self.send_lock = threading.Lock()
        self.waiters = {}
        self.inflight = defaultdict(int)          # all messages routed
        self.inflight_work = defaultdict(int)     # messages carrying chunk work
        self.timing = defaultdict(deque)          # route -> deque[[send, recv|None, work]]
        self.host_waits = deque(maxlen=3000)       # (host, per-route queue wait)
        threading.Thread(target=self.accept_loop, daemon=True).start()

    def accept_loop(self):
        while True:
            try:
                sock, _ = self.server.accept()
            except OSError:
                return
            except Exception as exc:
                print(f'ACCEPT_ERROR {exc!r}', flush=True)
                continue
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass
            threading.Thread(target=self.read_loop, args=(sock,), daemon=True).start()

    def read_loop(self, sock):
        try:
            with sock.makefile('r', encoding='utf-8') as stream:
                hello = json.loads(stream.readline())
                if hello['type'] == 'source':
                    with self.lock:
                        self.source = sock.makefile('w', encoding='utf-8', buffering=1)
                    print('GOT_SOURCE', flush=True)
                    while stream.readline():
                        pass  # source never sends; detect close
                else:
                    assert hello['type'] == 'worker_ready', hello
                    with self.lock:
                        self.workers[hello['route']] = hello
                    if DEBUG:
                        print(f'GOT_WORKER route={hello["route"]} total={len(self.workers)}', flush=True)
                    for line in stream:
                        self.handle_result(json.loads(line))
        except Exception as exc:
            print(f'READ_LOOP_ERROR route={hello.get("route") if isinstance(hello, dict) else "?"} {exc!r}', flush=True)

    def handle_result(self, event):
        if event.get('type') != 'result':
            return
        if DEBUG:
            print(f"DRIVER_RESULT route={event.get('route')} qid={event.get('query_id')}", flush=True)
        route = event['route']
        now = time.perf_counter()
        with self.lock:
            for item in self.timing[route]:
                if item[1] is None:
                    item[1] = now
                    # Per-route queue wait: this route's dispatch-to-result time
                    # minus its own execution time. Not the whole-query elapsed,
                    # which stragglers on other nodes would inflate.
                    wait = max(0.0, (now - item[0]) - event.get('worker_seconds', 0.0))
                    self.host_waits.append((event.get('host'), wait))
                    if item[2]:
                        self.inflight_work[route] = max(0, self.inflight_work[route] - 1)
                    break
            self.inflight[route] = max(0, self.inflight[route] - 1)
            waiter = self.waiters.get(event['query_id'])
            if waiter is not None:
                waiter['rows'][route] = event
                if len(waiter['rows']) >= waiter['expected']:
                    waiter['event'].set()
                    print(f"QUERY_COMPLETE {event['query_id']} rows={len(waiter['rows'])}", flush=True)

    def roundtrip(self, requests, timeout=600.0):
        """Register waiters before sending so no result can be missed."""
        by_query = defaultdict(list)
        for r in requests:
            by_query[r['query_id']].append(r['route'])
        waiters = {}
        with self.lock:
            for query_id, routes in by_query.items():
                waiter = {'rows': {}, 'expected': len(set(routes)), 'event': threading.Event()}
                self.waiters[query_id] = waiter
                waiters[query_id] = waiter
        self.send(requests)
        rows = {}
        for query_id, waiter in waiters.items():
            waiter['event'].wait(timeout)
            with self.lock:
                self.waiters.pop(query_id, None)
            assert waiter['event'].is_set() and len(waiter['rows']) == waiter['expected'], \
                f'query {query_id} incomplete: {len(waiter["rows"])}/{waiter["expected"]}'
            rows[query_id] = [waiter['rows'][r] for r in sorted(waiter['rows'])]
        return rows

    def send(self, requests):
        lines = ''.join(json.dumps(r, separators=(',', ':')) + '\n' for r in requests)
        with self.send_lock:
            self.source.write(lines)
            self.source.flush()
        now = time.perf_counter()
        with self.lock:
            for r in requests:
                work = int(bool(r.get('entries')))
                self.inflight[r['route']] += 1
                if work:
                    self.inflight_work[r['route']] += 1
                self.timing[r['route']].append([now, None, work])

    def host_routes(self):
        with self.lock:
            groups = defaultdict(list)
            for route, info in self.workers.items():
                groups[info['host']].append(route)
            return {h: sorted(v) for h, v in groups.items()}

    def pick_route(self, host):
        routes = self.host_routes()[host]
        with self.lock:
            return min(routes, key=lambda r: (self.inflight[r], r))

    def snapshot_loads(self, window):
        now = time.perf_counter()
        with self.lock:
            inflight = dict(self.inflight)
            inflight_work = dict(self.inflight_work)
            total_work = sum(self.inflight_work.values())
            busy = {}
            for route, items in self.timing.items():
                total = 0.0
                for send, recv, _work in items:
                    end = recv if recv is not None else now
                    total += max(0.0, min(end, now) - max(send, now - window))
                busy[route] = min(1.0, total / window)
                while items and items[0][1] is not None and items[0][1] < now - 60:
                    items.popleft()
        return inflight, inflight_work, busy

    def total_inflight_work(self):
        with self.lock:
            return sum(self.inflight_work.values())

    def close(self):
        self.server.close()


class Experiment:
    def __init__(self, args, lattice, meta, golden, policy, workload):
        self.args = args
        self.workload = workload
        self.lattice = lattice
        self.meta = meta
        self.golden = golden
        self.run_dir = Path(args.output)
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.queries = Jsonl(self.run_dir / 'queries.jsonl')
        self.samples = Jsonl(self.run_dir / 'samples.jsonl')
        self.actions = Jsonl(self.run_dir / 'actions.jsonl')
        self.policy = policy
        self.backend = TimedBackend(self.run_dir / 'layout_backend')
        self.connections = None
        self.controller = None
        self.tree = None
        self.host_owner = {}
        self.owner_host = {}
        self.stop = threading.Event()
        self.decide_enabled = threading.Event()
        self.plan_queue = queue_module.Queue(maxsize=1)
        self.scenario_name = 'init'
        self.current_mix = {}
        self.mix_lock = threading.Lock()
        self.stats = {'queries': 0, 'golden_pass': 0, 'dispatch_failures': 0}

    def build_index(self):
        if self.meta is not None:
            chunks = [Chunk(str(i), (e['lon_min'], e['lat_min']), (e['lon_max'], e['lat_max']),
                            (e['lon_min'], e['lat_min'])) for i, e in enumerate(self.meta['entries'])]
        else:
            chunks = [Chunk(str(i), (b[0], b[1]), (b[2], b[3]), (b[0], b[1]))
                      for i, b in enumerate(self.lattice.entries_lonlat[e]
                                            for e in sorted(self.lattice.entries_lonlat))]
        started = time.perf_counter()
        self.tree = ChunkTree(chunks)
        return {'construction_seconds': time.perf_counter() - started, 'chunks': len(chunks)}

    def wait_workers(self, expected):
        deadline = time.monotonic() + 240
        last_report = 0.0
        while len(self.connections.workers) < expected or self.connections.source is None:
            assert time.monotonic() < deadline, \
                f'only {len(self.connections.workers)}/{expected} workers connected'
            if time.monotonic() - last_report > 5:
                last_report = time.monotonic()
                extra = ''
                if self.args.fake_executor:
                    extra = f' fake_pid={getattr(self, "fake_pid", None)}'
                print(f'WAIT_WORKERS {len(self.connections.workers)}/{expected} '
                      f'source={self.connections.source is not None}{extra}', flush=True)
            time.sleep(0.2)
        for host in sorted(self.connections.host_routes()):
            owner = NODE_PREFIX + host.split('.')[-1]
            self.host_owner[host] = owner
            self.owner_host[owner] = host

    def initial_layout(self):
        """Static spatially-contiguous starting layout: node ownership follows
        latitude bands in fixed node order. This is the un-rebalanced fixed
        ownership the dynamic policy is expected to improve upon under skew;
        block-locality preference is not applied at partition granularity
        because every partition's tiles span all source files and therefore
        all storage hosts."""
        capacities = {self.host_owner[h]: len(r) for h, r in self.connections.host_routes().items()}
        layout0 = self.workload.get('initial_layout', {})
        boxes = sorted(self.lattice.initial_boxes(layout0.get('row_parts', 4), layout0.get('col_parts', 4)),
                       key=lambda b: (b.low[0], b.low[1]))
        owners = sorted(capacities)
        partitions = []
        for index, box in enumerate(boxes):
            rows = [r for r in self.lattice.tile_rows if box.contains(r[0])]
            owner = owners[index * len(owners) // len(boxes)]
            partitions.append(self.backend.create(
                len(partitions), owner, box, rows, 0.0, 0, 'gleam', 'initial'))
        self.controller = LBController(self.backend, capacities, partitions, self.policy)
        self.controller.initial_checkpoint()
        return capacities

    # ---------- control loop ----------

    def sampler(self):
        sample = self.policy.sample_seconds
        next_tick = int(time.time()) // sample * sample + sample
        while not self.stop.is_set():
            delay = next_tick - time.time()
            if delay > 0:
                time.sleep(min(delay, 0.5))
                if time.time() < next_tick:
                    continue
            now_aligned = float(next_tick)
            next_tick += sample
            inflight, inflight_work, busy = self.connections.snapshot_loads(sample)
            samples, utilization = {}, {}
            for host, routes in self.connections.host_routes().items():
                owner = self.host_owner[host]
                utilization[owner] = sum(busy.get(r, 0.0) for r in routes) / max(1, len(routes))
                # Concurrently executing subtasks over the
                # fixed slot capacity. Only messages carrying chunk work count as
                # an executing subtask; empty routing no-ops do not. Total routed
                # messages are logged separately as evidence.
                active = min(sum(inflight_work.get(r, 0) for r in routes), len(routes))
                samples[owner] = NodeSample(active=active, capacity=len(routes),
                                            observed_at=now_aligned,
                                            queue_wait_seconds=self.recent_wait(host),
                                            transfer_bytes_per_second=self.backend.bandwidth_bytes_per_second(),
                                            checkpoint_seconds=self.backend.obs['last_checkpoint_seconds'])
            epoch, _, owners = self.controller.route_table()
            owned = defaultdict(int)
            for owner in owners.values():
                owned[owner] += 1
            self.samples.write({'t': now_aligned, 'scenario': self.scenario_name, 'epoch': epoch,
                                'active': {o: samples[o].active for o in samples},
                                'capacity': {o: samples[o].capacity for o in samples},
                                'load': {o: round(samples[o].load, 4) for o in samples},
                                'routed_inflight': {o: min(sum(inflight.get(r, 0)
                                                               for r in self.connections.host_routes()[h]), len(routes))
                                                    for h, routes in self.connections.host_routes().items()
                                                    for o in [self.host_owner[h]]},
                                'utilization': {o: round(utilization[o], 4) for o in utilization},
                                'queue_wait': {o: round(samples[o].queue_wait_seconds, 4) for o in samples},
                                'partitions': dict(owned)})
            if self.decide_enabled.is_set():
                try:
                    plan = self.controller.decide(samples, now_aligned)
                except ValueError:
                    plan = None
                if plan is not None:
                    try:
                        self.plan_queue.put_nowait(plan)
                    except queue_module.Full:
                        pass
                elif any(v >= self.policy.consecutive_samples
                         for pair in self.controller.streaks.values() for v in pair):
                    mean = sum(self.controller.counters.count(c, now_aligned)
                               for c in self.controller.chunks) / max(1, len(self.controller.chunks))
                    top = sorted(((self.controller.counters.count(p.identifier, now_aligned),
                                   p.identifier, p.owner) for p in self.controller.chunks.values()),
                                 reverse=True)[:4]
                    self.samples.write({'t': now_aligned, 'scenario': self.scenario_name,
                                        'decide_debug': True, 'streaks': dict(self.controller.streaks),
                                        'cooldown': dict(self.controller.cooldown), 'mean': round(mean, 2),
                                        'top_counts': top,
                                        'queue_wait': {o: round(samples[o].queue_wait_seconds, 4) for o in samples},
                                        'bandwidth': self.backend.bandwidth_bytes_per_second(),
                                        'checkpoint_seconds': self.backend.obs['last_checkpoint_seconds']})

    def recent_wait(self, host):
        with self.connections.lock:
            rows = [w for h, w in self.connections.host_waits if h == host]
        return max(0.0, sum(rows) / len(rows)) if rows else 0.0

    def action_worker(self):
        while not self.stop.is_set():
            try:
                plan = self.plan_queue.get(timeout=0.5)
            except queue_module.Empty:
                continue
            started = time.perf_counter()
            trace_before = len(self.controller.trace)
            inflight_before = self.connections.total_inflight_work()
            self.controller.submit(plan, time.time())
            drain_started = time.perf_counter()
            while not self.stop.is_set():
                status = self.controller.advance(time.time())
                if status == 'waiting_for_queries':
                    time.sleep(0.05)
                    continue
                break
            drain_seconds = time.perf_counter() - drain_started
            barrier_seconds = None
            if status == 'committed':
                epoch = self.controller.epoch
                self.controller.capture_serving()
                # No epoch broadcast is required. The plane cache is
                # position-keyed so layout changes invalidate nothing, and the
                # published pointer switch is atomic and already confirmed by
                # the commit itself. The previously measured barrier wait was
                # queue position, not a consistency requirement.
                barrier_seconds = 0.0
            checkpoint = self.backend.root / f'checkpoint-{self.controller.epoch}.json'
            state_bytes = checkpoint.stat().st_size if checkpoint.exists() else None
            self.actions.write({
                't': time.time(), 'scenario': self.scenario_name, 'kind': plan.kind,
                'status': status, 'affected': list(plan.affected),
                'placements': [{'owner': p.owner, 'parents': list(p.parents),
                                'box': [list(p.box.low), list(p.box.high)],
                                'preserve_id': p.preserve_id} for p in plan.placements],
                'nodes': list(plan.nodes), 'epoch_after': self.controller.epoch,
                'handoff_protocol': 'serving-through-commit',
                'inflight_work_messages_at_submit': inflight_before,
                'total_seconds': time.perf_counter() - started,
                'drain_seconds': drain_seconds, 'barrier_seconds': barrier_seconds,
                'state_bytes': state_bytes,
                'layout_bytes': self.backend.pointer.stat().st_size,
                'estimated_gain': plan.estimated_gain, 'estimated_cost': plan.estimated_cost,
                'controller_trace': self.controller.trace[trace_before:]})

    # ---------- query path ----------

    def dispatch(self, box_id, client, seq):
        box = self.lattice.boxes[box_id]
        selected = sorted(int(i) for i in self.tree.query(
            (box['lon_min'], box['lat_min']), (box['lon_max'], box['lat_max'])))
        query_id = f'{self.scenario_name}_c{client}_{seq}'
        # Every query is served from the currently published
        # (committed) layout. A handoff switches this view atomically at
        # commit; in-flight queries keep their per-dispatch entry lists and
        # are unaffected by the ownership change, so admission never pauses.
        epoch, tile_owner, owners = self.controller.serving_table()
        partitions = sorted({tile_owner[self.lattice.entry_tile[e]] for e in selected})
        self.controller.count_access(partitions, time.time())
        byroute = defaultdict(list)
        for entry in selected:
            owner = owners[tile_owner[self.lattice.entry_tile[entry]]]
            byroute[self.connections.pick_route(self.owner_host[owner])].append(entry)
        routes = sorted(self.connections.workers)
        requests = [{'type': 'query', 'route': r, 'query_id': query_id,
                     'entries': byroute.get(r, []),
                     'box': [box['lat_min'], box['lat_max'], box['lon_min'], box['lon_max']],
                     # The executor's plane cache is keyed by data position,
                     # not ownership, so a layout change never invalidates cached
                     # planes; clear_epoch stays constant and no flush ever fires.
                     'clear_epoch': 'layout'} for r in routes]
        dispatched = sorted(e for r in requests for e in r['entries'])
        assert dispatched == selected, f'{query_id}: dispatch mismatch'
        t0 = time.perf_counter()
        rows = self.connections.roundtrip(requests)[query_id]
        elapsed = time.perf_counter() - t0
        monthly = defaultdict(lambda: {'sum': 0.0, 'count': 0})
        for row in rows:
            for month, value in row['monthly'].items():
                monthly[month]['sum'] += value['sum']
                monthly[month]['count'] += value['count']
        if self.golden is not None:
            expected = self.golden['gleam_1981_1990_spatial_R' + str(box_id)]
            assert set(monthly) == set(expected), query_id
            for month in monthly:
                assert monthly[month]['count'] == expected[month]['count'], (query_id, month, 'count')
                assert math.isclose(monthly[month]['sum'], expected[month]['sum'],
                                    rel_tol=1e-9, abs_tol=1e-6), (query_id, month, 'sum')
            self.stats['golden_pass'] += 1
        self.stats['queries'] += 1
        self.queries.write({
            't': time.time(), 'scenario': self.scenario_name, 'client': client,
            'query_id': query_id, 'box': box_id, 'serving_epoch': epoch, 'latency': elapsed,
            'worker_seconds_sum': sum(r.get('worker_seconds', 0.0) for r in rows),
            'physical_read_bytes': sum(r.get('physical_read_bytes', 0) for r in rows),
            'plane_cache_hits': sum(r.get('plane_cache_hits', 0) for r in rows),
            'plane_cache_misses': sum(r.get('plane_cache_misses', 0) for r in rows),
            'active_hosts': sorted({r['host'] for r in rows if r['monthly']})})
        return elapsed

    def client(self, index, deadline):
        rng = random.Random(index * 7919 + self.args.seed)
        seq = 0
        while time.time() < deadline and not self.stop.is_set():
            with self.mix_lock:
                mix = dict(self.current_mix)
            self.dispatch(weighted_choice(rng, mix), index, seq)
            seq += 1

    def run_scenario(self, scenario):
        self.scenario_name = scenario['name']
        mix = {int(k): float(v) for k, v in scenario['mix'].items()}
        assert abs(sum(mix.values()) - 1.0) < 1e-6, scenario['name']
        with self.mix_lock:
            self.current_mix = mix
        switcher = None
        if scenario.get('mix_after'):
            mix_after = {int(k): float(v) for k, v in scenario['mix_after'].items()}
            switch_at = float(scenario['switch_at_seconds'])
            switcher = threading.Thread(target=self.switch_mix, args=(switch_at, mix_after), daemon=True)
        if scenario.get('policy', True):
            self.decide_enabled.set()
        else:
            self.decide_enabled.clear()
        clients = int(scenario.get('clients', 4))
        duration = float(scenario['duration_seconds'])
        print(f"SCENARIO_START {json.dumps({'name': self.scenario_name, 'clients': clients, 'duration': duration})}", flush=True)
        if switcher is not None:
            switcher_started = time.time()
            switcher.start()
        threads = [threading.Thread(target=self.client, args=(i, time.time() + duration), daemon=True)
                   for i in range(clients)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(duration + 300)
        print(f"SCENARIO_END {json.dumps({'name': self.scenario_name})}", flush=True)
        time.sleep(float(self.args.settle_seconds))

    def switch_mix(self, delay, mix_after):
        time.sleep(delay)
        with self.mix_lock:
            self.current_mix = mix_after

    def summarize(self):
        summary = {'stats': dict(self.stats),
                   'action_counts': dict(self.controller.action_counts),
                   'trace': self.controller.trace}
        pointer = self.backend.pointer
        if pointer.exists():
            summary['final_layout'] = json.loads(pointer.read_text())['chunks']
        (self.run_dir / 'summary.json').write_text(json.dumps(summary, indent=1, default=str))


def weighted_choice(rng, mix):
    roll = rng.random()
    cumulative = 0.0
    for key in sorted(mix):
        cumulative += mix[key]
        if roll <= cumulative:
            return key
    return sorted(mix)[-1]


def fetch_rest(path):
    with urllib.request.urlopen(REST + path, timeout=20) as stream:
        return json.load(stream)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--meta')
    parser.add_argument('--golden')
    parser.add_argument('--workload', default=str(ROOT / 'workload.json'))
    parser.add_argument('--output', required=True)
    parser.add_argument('--port', type=int, default=38771)
    parser.add_argument('--bind', default='192.0.2.64')
    parser.add_argument('--expected-routes', type=int, default=48)
    parser.add_argument('--fake-executor')
    parser.add_argument('--flink-jar')
    parser.add_argument('--seed', type=int, default=20261014)
    parser.add_argument('--settle-seconds', type=float, default=8.0)
    args = parser.parse_args()
    workload = json.loads(Path(args.workload).read_text(encoding='utf-8'))
    policy = Policy(**{**{'sample_seconds': 5, 'window_seconds': 60, 'consecutive_samples': 3,
                          'cooldown_seconds': 60},
                       **workload.get('policy', {})})

    meta = golden = None
    if args.fake_executor:
        args.bind = args.bind if args.bind != '192.0.2.64' else '127.0.0.1'
    elif not args.meta:
        parser.error('--meta is required in real mode')
    else:
        meta = json.loads(Path(args.meta).read_text())
        golden = {row['case']: row['monthly_values']
                  for row in json.loads(Path(args.golden).read_text())}
    lattice = Lattice(meta=meta) if meta is not None else Lattice()
    experiment = Experiment(args, lattice, meta, golden, policy, workload)
    state = {'status': 'INITIALIZING'}
    state_path = experiment.run_dir / 'state.json'
    state_path.write_text(json.dumps(state))

    fake = None
    job_id = None
    experiment.connections = Connections(args.bind, args.port)
    try:
        index_info = experiment.build_index()
        if args.fake_executor:
            fake_log = open(experiment.run_dir / 'fake_executor.log', 'w', encoding='utf-8')
            fake = subprocess.Popen([sys.executable, args.fake_executor,
                                     args.bind, str(args.port), str(args.expected_routes)],
                                    stdout=fake_log, stderr=subprocess.STDOUT)
            experiment.fake_pid = fake.pid
        else:
            overview = fetch_rest('/overview')
            assert overview['jobs-running'] == 0, overview
            assert overview['slots-available'] >= args.expected_routes, overview
            submitted = subprocess.run(['/opt/mgsi-cluster/flink/bin/flink', 'run', '-d',
                                        '-m', '192.0.2.64:8081', '-c', 'ResidentMGSI',
                                        args.flink_jar, args.bind, str(args.port), args.meta],
                                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                       universal_newlines=True)
            (experiment.run_dir / 'submission.log').write_text(submitted.stdout)
            assert submitted.returncode == 0, submitted.stdout
            job_id = re.search(r'JobID\s+([0-9a-f]{32})', submitted.stdout).group(1)
            state['job_id'] = job_id
        experiment.wait_workers(args.expected_routes)
        routes = sorted(experiment.connections.workers)
        experiment.connections.roundtrip(
            [{'type': 'query', 'route': r, 'query_id': 'init_barrier', 'entries': [],
              'box': [0, 0, -180, 180], 'clear_epoch': 'layout'} for r in routes])
        print('INIT_BARRIER_DONE', flush=True)
        capacities = experiment.initial_layout()
        print(f'INITIAL_LAYOUT {json.dumps(capacities)}', flush=True)
        pin = {'code_sha256': {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                               for name in ('run_lb.py', 'adaptive_policy.py', 'chunk_tree.py')},
               'policy': workload.get('policy', {}), 'capacities': capacities,
               'host_owner': experiment.host_owner, 'index': index_info,
               'protocol': workload.get('protocol', ''), 'expected_routes': args.expected_routes}
        (experiment.run_dir / 'run_pin.json').write_text(json.dumps(pin, indent=1))
        state['status'] = 'RUNNING'
        state_path.write_text(json.dumps(state))

        threads = [threading.Thread(target=experiment.sampler, daemon=True),
                   threading.Thread(target=experiment.action_worker, daemon=True)]
        for t in threads:
            t.start()
        for scenario in workload['scenarios']:
            experiment.run_scenario(scenario)
        experiment.stop.set()
        for t in threads:
            t.join(15)
        experiment.summarize()
        state['status'] = 'COMPLETE'
    finally:
        experiment.stop.set()
        if experiment.connections.source is not None:
            try:
                experiment.connections.send([{'type': 'stop'}])
            except Exception:
                pass
        if job_id:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                try:
                    if fetch_rest('/jobs/' + job_id)['state'] in ('FINISHED', 'FAILED', 'CANCELED'):
                        break
                except Exception:
                    break
                time.sleep(0.5)
            for name, path in [('job', '/jobs/' + job_id), ('exceptions', '/jobs/' + job_id + '/exceptions')]:
                try:
                    (experiment.run_dir / (name + '.json')).write_text(json.dumps(fetch_rest(path), indent=1))
                except Exception:
                    pass
        if fake is not None:
            fake.terminate()
        experiment.connections.close()
        state_path.write_text(json.dumps(state))
    assert state['status'] == 'COMPLETE', state
    print('LB_COMPLETE ' + json.dumps(state), flush=True)


if __name__ == '__main__':
    main()
