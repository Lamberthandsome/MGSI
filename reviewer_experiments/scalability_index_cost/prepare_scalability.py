"""Stage the scalability directory on the server (run with PY312 on DL1).

Compiles the parallelism-parameterized jar and copies the verified GLEAM v2
inputs (physical index, R-box golden, yangtze/global golden).
"""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent
SOURCE = Path('/opt/mgsi-experiments/20261015-gleam-ablation-v2')


def main():
    assert not (ROOT/'preparation_complete.json').exists(), 'Already prepared.'
    for name in ('physical_index.with_blocks.json', 'golden_results.json', 'golden_extra.json'):
        shutil.copyfile(SOURCE/name, ROOT/name)
    meta = json.loads((ROOT/'physical_index.with_blocks.json').read_text())
    assert len(meta['files']) == 10 and len(meta['entries']) == 5000

    cp = ':'.join(['/opt/mgsi-cluster/flink/lib/*', '/app/hadoop-2.7.2/share/hadoop/common/*',
        '/app/hadoop-2.7.2/share/hadoop/common/lib/*', '/app/hadoop-2.7.2/share/hadoop/hdfs/*',
        '/app/hadoop-2.7.2/share/hadoop/hdfs/lib/*'])
    classes = ROOT/'classes_mgsi'
    if classes.exists():
        shutil.rmtree(classes)
    classes.mkdir()
    subprocess.run(['/opt/mgsi-modern/jdk17/bin/javac', '--release', '8', '-cp', cp, '-d', str(classes), str(ROOT/'ResidentMGSI.java')], check=True)
    subprocess.run(['/opt/mgsi-modern/jdk17/bin/jar', 'cf', str(ROOT/'resident-mgsi.jar'), '-C', str(classes), '.'], check=True)

    pins = {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
            ('ResidentMGSI.java', 'resident-mgsi.jar', 'run_scalability.py', 'physical_index.with_blocks.json')}
    (ROOT/'preparation_complete.json').write_text(json.dumps(
        {'status': 'READY', 'source': str(SOURCE), 'sha256': pins,
         'scope': 'P57(b): latency + pipelined throughput at 1/2/4/8 TaskManagers; volume slices 1/5 yr at 8 TM',
         'new_hdfs_outputs': False}, indent=2))
    print('PREPARATION_READY[scalability]', flush=True)


if __name__ == '__main__':
    main()
