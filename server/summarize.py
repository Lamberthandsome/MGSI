"""Validate complete returned results and retain all timings before summaries."""
from pathlib import Path
import argparse
import csv
import hashlib
import json
import math
import statistics

ROOT=Path(__file__).resolve().parent

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--stage-only',action='store_true'); args=parser.parse_args()
    dirs=[ROOT] if args.stage_only else [ROOT/x for x in ['gleam_mgsi','gleam_native','gleam_zarr','gleam_sedona']]
    all_rows=[]; verified=[]
    for directory in dirs:
        files=([directory/'results.jsonl'] if directory.name=='gleam_mgsi' else
            list(directory.glob('*_results.jsonl')))
        assert files and all(p.exists() for p in files),str(directory)
        reference=json.loads((directory/'gleam_reference.json').read_text())
        for path in files:
            rows=[json.loads(line) for line in path.read_text().splitlines() if line.strip()]
            assert len(rows)==48,(path,len(rows))
            method='mgsi' if path.name=='results.jsonl' else rows[0]['method']
            for region in range(1,9):
                selected=[r for r in rows if r['region']==region]
                assert len(selected)==6 and sum(r['mode']=='warmup' for r in selected)==1
                measured=[r for r in selected if r['mode']=='measured']; assert len(measured)==5
                values=[r['until_final_120_month_results_seconds'] for r in measured]
                assert all(math.isfinite(v) and v>0 for v in values)
                for row in selected:
                    assert row['status']=='PASS' and set(row['active_data_hosts'])=={'192.0.2.65','192.0.2.66','192.0.2.71','192.0.2.72'}
                    observed=row['monthly_values']; expected=reference[str(region)]
                    assert observed.keys()==expected.keys() and len(observed)==120
                    for month,e in expected.items():
                        a=observed[month]
                        assert a['count']==e['count'] and math.isclose(a['sum'],e['sum'],rel_tol=1e-9,abs_tol=1e-6)
                all_rows.append({'dataset':'gleam','method':method,'region':region,
                    'mean_seconds':statistics.mean(values),'stddev_seconds':statistics.stdev(values),
                    'measured_repeats':5,'warmup_repeats_separate':1,'raw_measured_seconds':values})
            verified.append({'file':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'queries':len(rows)})
    if not args.stage_only: assert len(all_rows)==32 and len(verified)==4
    (ROOT/'verified_summary.json').write_text(json.dumps({'status':'PASS','files':verified,'cells':all_rows,
        'scope':'GLEAM spatial first batch'},indent=2))
    with (ROOT/'query_statistics.csv').open('w',newline='') as out:
        names=['dataset','method','region','mean_seconds','stddev_seconds','measured_repeats','warmup_repeats_separate','raw_measured_seconds']
        w=csv.DictWriter(out,fieldnames=names); w.writeheader(); w.writerows(all_rows)
    print('RETURNED_RESULT_VERIFICATION_PASS '+json.dumps({'cells':len(all_rows),'queries':len(verified)*48,'monthly_results':len(verified)*48*120}),flush=True)

if __name__=='__main__': main()
