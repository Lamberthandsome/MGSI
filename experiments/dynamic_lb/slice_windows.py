"""Slice a run into 60 s buckets and compare quiet windows (>=120 s after the
last committed action) against action-adjacent windows: throughput, p50,
cache misses per query. Explains where the ON-policy throughput gap lives."""
import json
import statistics
import sys
from pathlib import Path


def load(path):
    with open(path, encoding='utf-8') as stream:
        return [json.loads(line) for line in stream if line.strip()]


for run in sys.argv[1:]:
    base = Path(run)
    queries = load(base / 'queries.jsonl')
    actions = [a['t'] for a in load(base / 'actions.jsonl')
               if a.get('status') == 'committed']
    t0 = min(q['t'] - q['latency'] for q in queries)
    print('=' * 20, run, f'n={len(queries)} actions={len(actions)}')
    bucket_seconds = 60
    buckets = {}
    for q in queries:
        key = int((q['t'] - q['latency'] - t0) // bucket_seconds)
        buckets.setdefault(key, []).append(q)
    rows = []
    for key in sorted(buckets):
        group = buckets[key]
        latencies = sorted(q['latency'] for q in group)
        misses = sum(q.get('plane_cache_misses', 0) for q in group) / len(group)
        end = t0 + (key + 1) * bucket_seconds
        last_act = max((t for t in actions if t < end), default=None)
        quiet = last_act is None or (end - last_act) >= 120
        rows.append((key, len(group) / bucket_seconds,
                     latencies[len(latencies) // 2], misses, quiet))
    for key, qps, p50, miss, quiet in rows:
        print(f'min{key:02d} qps={qps:5.2f} p50={p50:6.2f} '
              f'miss/q={miss:6.1f} {"QUIET" if quiet else ""}')
    quiet_rows = [r for r in rows if r[4]]
    busy_rows = [r for r in rows if not r[4]]
    if quiet_rows and busy_rows:
        q_q = statistics.mean(r[1] for r in quiet_rows)
        b_q = statistics.mean(r[1] for r in busy_rows)
        q_m = statistics.mean(r[3] for r in quiet_rows)
        b_m = statistics.mean(r[3] for r in busy_rows)
        q_p = statistics.median(r[2] for r in quiet_rows)
        b_p = statistics.median(r[2] for r in busy_rows)
        print(f'>>> QUIET buckets={len(quiet_rows)}: '
              f'qps={q_q:.2f} p50={q_p:.2f} miss/q={q_m:.1f}')
        print(f'>>> BUSY  buckets={len(busy_rows)}: '
              f'qps={b_q:.2f} p50={b_p:.2f} miss/q={b_m:.1f}')
