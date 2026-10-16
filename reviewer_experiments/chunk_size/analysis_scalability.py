"""Summarize scalability results: per label, latency mean/std per case and
throughput; writes analysis_scalability.json and prints a table."""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def main():
    labels = sys.argv[1:] or sorted(p.name[len('results_'):-len('.jsonl')] for p in ROOT.glob('results_*.jsonl'))
    cells = {}
    tp = {}
    for label in labels:
        rows = [json.loads(l) for l in (ROOT/('results_%s.jsonl' % label)).read_text().splitlines()]
        assert all(r['status'] == 'PASS' for r in rows)
        measured = [r for r in rows if r.get('phase') == 'latency' and r['mode'] == 'measured']
        groups = defaultdict(list)
        for r in measured:
            groups[r['case']].append(r['seconds'])
        for case, values in groups.items():
            cells['{}/{}'.format(label, case)] = {
                'seconds': {'mean': statistics.mean(values), 'sample_std': statistics.stdev(values),
                            'reps': len(values), 'values': values},
                'physical_read_bytes_mean': statistics.mean(
                    r['physical_read_bytes'] for r in measured if r['case'] == case)}
        for r in rows:
            if r.get('phase') == 'throughput':
                tp[label] = {'qps': r['qps'], 'completed': r['completed_queries'],
                             'seconds': r['seconds'], 'in_flight': r['in_flight']}
    out = {'cells': cells, 'throughput': tp}
    (ROOT/'analysis_scalability.json').write_text(json.dumps(out, indent=2))
    print('label/case                    mean_s   std_s')
    for key in sorted(cells):
        c = cells[key]['seconds']
        print('{:28s} {:7.2f} {:6.2f}'.format(key, c['mean'], c['sample_std']))
    for label in sorted(tp):
        print('THROUGHPUT {:10s} {:.3f} qps ({} queries / {:.0f}s)'.format(
            label, tp[label]['qps'], tp[label]['completed'], tp[label]['seconds']))


if __name__ == '__main__':
    main()
