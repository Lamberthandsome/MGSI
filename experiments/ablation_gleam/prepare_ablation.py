"""Stage an ablation batch directory on the server (run with Python 3.12).

Usage: python prepare_ablation.py gleam|merra2

1. Refuse to overwrite an already-prepared directory.
2. Copy the verified physical index and R-box golden results from the matched
   stage; verify the dataset's file/entry counts and record sha256 pins.
3. Substitute the dataset name into ResidentMGSI.java, compile, jar.
4. Build golden_extra.json (yangtze + global cases) with the R1-R8 self-check.
5. Distribute the physical index to the four workers at the same path (md5).
6. HDFS health check; write preparation_complete.json.
"""
from pathlib import Path
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent
PY312 = '/opt/mgsi-modern/python312/bin/python'
WORKERS = ['192.0.2.65', '192.0.2.66', '192.0.2.71', '192.0.2.72']

SOURCES = {
    'gleam':  {'bundle': '/opt/mgsi-experiments/matched_baselines_90pct', 'stage': 'gleam_mgsi',
               'files': 10, 'entries': 5000},
    'merra2': {'bundle': '/opt/mgsi-experiments/matched_baselines_95pct', 'stage': 'merra2_spatial',
               'files': 120, 'entries': 480},
}


def health(path):
    url = 'http://192.0.2.64:50070/fsck?' + urllib.parse.urlencode({'path': path, 'user.name': 'root'})
    with urllib.request.urlopen(url, timeout=150) as stream:
        text = stream.read(16*1024**2).decode()
    assert 'Status: HEALTHY' in text or ' is HEALTHY' in text, (path, text[-3000:])
    return {'path': path, 'checked_at': time.time()}


def libhdfs_env():
    java_home = '/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.412.b08-1.el7_9.x86_64'
    os.environ['JAVA_HOME'] = java_home
    os.environ['LD_LIBRARY_PATH'] = '/app/hadoop-2.7.2/lib/native:' + java_home + '/jre/lib/amd64/server'
    os.environ['CLASSPATH'] = '/app/hadoop-2.7.2/share/hadoop/hdfs/*:/app/hadoop-2.7.2/share/hadoop/common/*:/app/hadoop-2.7.2/share/hadoop/common/lib/*'


def main():
    dataset = sys.argv[1] if len(sys.argv) > 1 else 'gleam'
    spec = SOURCES[dataset]
    assert not (ROOT/'preparation_complete.json').exists(), 'Already prepared; archive this directory first.'
    source = Path(spec['bundle'])
    for name in ('physical_index.with_blocks.json', 'golden_results.json'):
        found = next((p for p in (source/name, source/spec['stage']/name) if p.exists()), None)
        assert found, name
        shutil.copyfile(found, ROOT/name)
    meta = json.loads((ROOT/'physical_index.with_blocks.json').read_text())
    assert len(meta['files']) == spec['files'] and len(meta['entries']) == spec['entries']
    golden = json.loads((ROOT/'golden_results.json').read_text())
    assert {row['case'] for row in golden} == {'{}_spatial_R{}'.format(
        next(iter({r['case'] for r in golden})).rsplit('_spatial_', 1)[0], r) for r in range(1, 9)}

    java = ROOT/'ResidentMGSI.java'
    text = java.read_text()
    if '__DATASET__' in text:
        java.write_text(text.replace('__DATASET__', dataset))
    else:
        assert 'MGSI ablation variants {}'.format(dataset) in text

    cp = ':'.join(['/opt/mgsi-cluster/flink/lib/*', '/app/hadoop-2.7.2/share/hadoop/common/*',
        '/app/hadoop-2.7.2/share/hadoop/common/lib/*', '/app/hadoop-2.7.2/share/hadoop/hdfs/*',
        '/app/hadoop-2.7.2/share/hadoop/hdfs/lib/*'])
    classes = ROOT/'classes_mgsi'
    if classes.exists():
        shutil.rmtree(classes)
    classes.mkdir()
    subprocess.run(['/opt/mgsi-modern/jdk17/bin/javac', '--release', '8', '-cp', cp, '-d', str(classes), str(java)], check=True)
    subprocess.run(['/opt/mgsi-modern/jdk17/bin/jar', 'cf', str(ROOT/'resident-mgsi.jar'), '-C', str(classes), '.'], check=True)

    libhdfs_env()
    if not (ROOT/'golden_extra.json').exists():
        subprocess.run([PY312, str(ROOT/'make_golden_extra.py'), dataset], check=True)

    meta_path = ROOT/'physical_index.with_blocks.json'
    local_md5 = subprocess.run(['md5sum', str(meta_path)], check=True, stdout=subprocess.PIPE, universal_newlines=True).stdout.split()[0]
    for worker in WORKERS:
        subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', 'root@'+worker,
                        'mkdir -p '+str(ROOT)], check=True)
        subprocess.run(['scp', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', str(meta_path),
                        'root@'+worker+':'+str(meta_path)], check=True)
        remote = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', 'root@'+worker,
                        'md5sum '+str(meta_path)], check=True, stdout=subprocess.PIPE, universal_newlines=True).stdout.split()[0]
        assert remote == local_md5, (worker, remote, local_md5)

    checks = [health(str(Path(meta['files'][0]['path']).parent))]
    pins = {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
            ('ResidentMGSI.java', 'resident-mgsi.jar', 'run_ablation.py', 'physical_index.with_blocks.json',
             'golden_results.json', 'golden_extra.json')}
    (ROOT/'input_health.json').write_text(json.dumps(checks, indent=2))
    (ROOT/'preparation_complete.json').write_text(json.dumps(
        {'status': 'READY', 'dataset': dataset, 'source_bundle': str(source), 'sha256': pins,
         'meta_md5_all_nodes': local_md5,
         'scope': 'seven ablation variants; R1-R8 plus temporal_yangtze (102-112E, 27-34N) and temporal_global',
         'new_hdfs_outputs': False}, indent=2))
    print('PREPARATION_READY[{}]: verified inputs, jar built, extra golden verified, meta distributed'.format(dataset), flush=True)


if __name__ == '__main__':
    main()
