"""Summarize the GLEAM ablation results (run on the server after the batch).

Reads results.jsonl, computes per (arm, variant, case) mean/sample-std of the five
measured repeats plus IO/cache counters, degradation relative to "full" within the
same arm and case, and the additivity contrasts (full vs global_only vs no_all_three).
Writes analysis.json; prints a compact table.
"""
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VARIANT_ORDER = ['full', 'no_chunking', 'no_global', 'no_local', 'no_inmemory', 'no_all_three', 'global_only']


def main():
    rows = [json.loads(line) for line in (ROOT/'results.jsonl').read_text().splitlines()]
    assert all(r['status'] == 'PASS' for r in rows)
    measured = [r for r in rows if r['mode'] == 'measured']
    assert len(rows) == len(measured) + len(rows)/6, 'repeat structure broken'

    groups = defaultdict(list)
    for r in measured:
        groups[(r['arm'], r['variant'], r['case'])].append(r)

    cells = {}
    for (arm, variant, case), items in groups.items():
        items = sorted(items, key=lambda r: r['repeat'])
        assert [r['repeat'] for r in items] == [1, 2, 3, 4, 5]
        seconds = [r['until_final_120_month_results_seconds'] for r in items]
        cells[(arm, variant, case)] = {
            'queries_total_pass': len([r for r in rows if r['variant'] == variant and r['case'] == case
                                       and r['arm'] == arm and r['status'] == 'PASS']),
            'seconds': {'mean': statistics.mean(seconds), 'sample_std': statistics.stdev(seconds),
                        'values': seconds},
            'physical_read_bytes': {'mean': statistics.mean(r['physical_read_bytes'] for r in items)},
            'physical_read_requests': {'mean': statistics.mean(r['physical_read_requests'] for r in items)},
            'plane_cache_hits': {'mean': statistics.mean(r['plane_cache_hits'] for r in items)},
            'plane_cache_misses': {'mean': statistics.mean(r['plane_cache_misses'] for r in items)},
        }

    contrasts = {}
    for (arm, variant, case), cell in sorted(cells.items()):
        if variant == 'full':
            continue
        base = cells[(arm, 'full', case)]['seconds']['mean']
        cell['seconds']['increase_vs_full_percent'] = (cell['seconds']['mean']-base)/base*100.0
    for arm in sorted({k[0] for k in cells}):
        for case in sorted({k[2] for k in cells if k[0] == arm}):
            key = '{}/{}'.format(arm, case)
            if all((arm, v, case) in cells for v in ('full', 'global_only', 'no_all_three')):
                f = cells[(arm, 'full', case)]['seconds']['mean']
                g = cells[(arm, 'global_only', case)]['seconds']['mean']
                a = cells[(arm, 'no_all_three', case)]['seconds']['mean']
                contrasts[key] = {
                    'full_seconds': f, 'global_only_seconds': g, 'no_all_three_seconds': a,
                    'local_plus_inmemory_joint_percent': (g-f)/f*100.0,
                    'all_layers_joint_percent': (a-f)/f*100.0,
                    'global_selection_share_percent_of_joint': (a-g)/f*100.0 if a >= g else 0.0,
                }

    out = {
        'scope': 'GLEAM Eb 1981-1990 monthly 15th; seven variants; R1-R8 + temporal_global; '
                 'arms cleared (main protocol) and retained (warm repeats); 1 warmup + 5 measured each',
        'total_queries': len(rows), 'all_pass': True,
        'cells': {'{}/{}/{}'.format(*k): v for k, v in sorted(cells.items())},
        'contrasts': contrasts,
    }
    (ROOT/'analysis.json').write_text(json.dumps(out, indent=2))

    print('arm/variant/case                      mean_s   std_s   vs_full   reads_GB')
    for (arm, variant, case), cell in sorted(cells.items()):
        inc = cell['seconds'].get('increase_vs_full_percent')
        print('{:37s} {:7.2f} {:6.2f} {:>8s} {:9.2f}'.format(
            '{}/{}/{}'.format(arm, variant, case), cell['seconds']['mean'], cell['seconds']['sample_std'],
            ('{:7.1f}%'.format(inc) if inc is not None else '      --'),
            cell['physical_read_bytes']['mean']/1e9))
    print(json.dumps(contrasts, indent=2))


if __name__ == '__main__':
    sys.exit(main())
