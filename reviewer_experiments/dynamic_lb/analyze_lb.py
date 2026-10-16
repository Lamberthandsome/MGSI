"""Evidence analyzer for dynamic load-balancing runs.

Turns queries.jsonl / samples.jsonl / actions.jsonl into the paper-ready
metric bundle: per-scenario throughput and latency, node-load imbalance over
time (pre/post first action), action trace with migration cost breakdown,
golden-verification counts, and cache statistics.
"""
import argparse
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


def rows(path):
    with open(path, encoding='utf-8') as stream:
        return [json.loads(line) for line in stream if line.strip()]


def pct(values, p):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * p))]


def load_spread(load):
    """max/min node load ratio at one sample (min floored to avoid div-zero)."""
    values = [v for v in load.values()]
    if not values:
        return None
    lo = max(min(values), 0.01)
    return round(max(values) / lo, 2)


def summarize_run(run_dir):
    run_dir = Path(run_dir)
    queries = rows(run_dir / 'queries.jsonl')
    samples = rows(run_dir / 'samples.jsonl')
    actions = rows(run_dir / 'actions.jsonl')
    sample_rows = [s for s in samples if 'load' in s]
    summary_path = run_dir / 'summary.json'
    summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    golden_pass = summary.get('stats', {}).get('golden_pass', 0)
    # Queries are only recorded after the golden assertion passes; the driver's
    # own counter in summary.json is authoritative when present.
    if not golden_pass and queries:
        golden_pass = len(queries)
    report = {'run_dir': str(run_dir), 'queries_total': len(queries),
              'golden_pass': golden_pass,
              'scenarios': {}, 'actions': actions}

    by_scenario = defaultdict(list)
    for q in queries:
        by_scenario[q['scenario']].append(q)
    samples_by = defaultdict(list)
    for s in sample_rows:
        samples_by[s['scenario']].append(s)

    for name, qs in sorted(by_scenario.items()):
        ss = samples_by.get(name, [])
        latencies = [q['latency'] for q in qs]
        span = (min(q['t'] for q in qs), max(q['t'] for q in qs)) if qs else (None, None)
        duration = (span[1] - span[0]) if qs and span[1] > span[0] else None
        entry = {
            'queries': len(qs),
            'duration_seconds': round(duration, 1) if duration else None,
            'throughput_qps': round(len(qs) / duration, 3) if duration else None,
            'latency_p50': round(pct(latencies, 0.5), 3) if latencies else None,
            'latency_p90': round(pct(latencies, 0.9), 3) if latencies else None,
            'latency_max': round(max(latencies), 3) if latencies else None,
            'cache_hits': sum(q.get('plane_cache_hits', 0) for q in qs),
            'cache_misses': sum(q.get('plane_cache_misses', 0) for q in qs),
            'physical_read_bytes': sum(q.get('physical_read_bytes', 0) for q in qs),
        }
        if ss:
            first_action_t = min((a['t'] for a in actions
                                  if a['scenario'] == name and a['status'] == 'committed'), default=None)
            pre = [s for s in ss if first_action_t is None or s['t'] < first_action_t]
            post = [s for s in ss if first_action_t is not None and s['t'] > first_action_t + 60]
            for label, group in (('load_pre_action', pre), ('load_post_action', post)):
                if group:
                    per_node = defaultdict(list)
                    for s in group:
                        for node, load in s['load'].items():
                            per_node[node].append(load)
                    entry[label] = {
                        'samples': len(group),
                        'mean_load': {n: round(statistics.mean(v), 3) for n, v in per_node.items()},
                        'max_load': {n: round(max(v), 3) for n, v in per_node.items()},
                        'mean_spread_ratio': round(statistics.mean(
                            [load_spread(s['load']) for s in group]), 2),
                    }
            entry['load_timeline'] = [
                {'t': round(s['t'], 0), 'load': s['load'],
                 'spread': load_spread(s['load']), 'epoch': s.get('epoch')}
                for s in ss[::max(1, len(ss) // 40)]]
        report['scenarios'][name] = entry

    committed = [a for a in actions if a['status'] == 'committed']
    report['action_summary'] = {
        'total_committed': len(committed),
        'by_kind': {kind: sum(1 for a in committed if a['kind'] == kind)
                    for kind in {a['kind'] for a in committed}},
        'drain_seconds': [round(a['drain_seconds'], 2) for a in committed],
        'barrier_seconds': [round(a['barrier_seconds'], 2) for a in committed
                            if a['barrier_seconds'] is not None],
        'state_bytes': [a['state_bytes'] for a in committed if a['state_bytes']],
        'migrations': [{'from_parents': a['placements'][0]['parents'] if a['placements'] else [],
                        'to_owner': a['placements'][0]['owner'] if a['placements'] else None,
                        'gain': round(a['estimated_gain'], 2),
                        'cost': round(a['estimated_cost'], 4),
                        'drain_seconds': round(a['drain_seconds'], 2),
                        'barrier_seconds': round(a['barrier_seconds'], 2)
                        if a['barrier_seconds'] is not None else None,
                        'state_bytes': a['state_bytes']}
                       for a in committed if a['kind'] == 'migrate'],
        'drain_timeouts': sum(1 for a in actions if a['status'] == 'drain_timeout'),
    }
    # Oscillation check: a partition migrated to X and back within cooldown.
    moves = [(a['t'], tuple(p['parents']), p['owner'])
             for a in committed for p in a['placements']]
    pingpong = []
    for i, (t1, parents1, owner1) in enumerate(moves):
        for t2, parents2, owner2 in moves[i + 1:]:
            if set(parents1) & set(parents2) and t2 - t1 < 120:
                pingpong.append((round(t1), round(t2), parents1, owner1, owner2))
    report['action_summary']['rapid_reversals_under_120s'] = pingpong
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_dir')
    parser.add_argument('--output', default=None)
    args = parser.parse_args()
    report = summarize_run(args.run_dir)
    out = args.output or (Path(args.run_dir) / 'analysis.json')
    Path(out).write_text(json.dumps(report, indent=1, ensure_ascii=False))
    print(json.dumps({k: v for k, v in report.items()
                      if k in ('queries_total', 'golden_pass', 'action_summary')},
                     indent=1, ensure_ascii=False))
    for name, entry in report['scenarios'].items():
        print(name, json.dumps({k: entry.get(k) for k in
                                ('queries', 'throughput_qps', 'latency_p50', 'latency_p90')},
                               ensure_ascii=False))


if __name__ == '__main__':
    main()
