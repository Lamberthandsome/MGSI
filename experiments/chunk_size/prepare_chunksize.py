"""Stage the chunk-size directory (server, PY312): inputs + jar + NCEP SQL meta."""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent
NCEP_STAGE = Path('/opt/mgsi-experiments/matched_baselines_95pct/ncep_spatial')
SQL_DIR = Path('/opt/mgsi-experiments/sql_full_archive')


def compile_jar(root):
    cp = ':'.join(['/opt/mgsi-cluster/flink/lib/*', '/app/hadoop-2.7.2/share/hadoop/common/*',
        '/app/hadoop-2.7.2/share/hadoop/common/lib/*', '/app/hadoop-2.7.2/share/hadoop/hdfs/*',
        '/app/hadoop-2.7.2/share/hadoop/hdfs/lib/*'])
    classes = root/'classes_mgsi'
    if classes.exists():
        shutil.rmtree(classes)
    classes.mkdir()
    subprocess.run(['/opt/mgsi-modern/jdk17/bin/javac', '--release', '8', '-cp', cp, '-d', str(classes), str(root/'ResidentMGSI.java')], check=True)
    subprocess.run(['/opt/mgsi-modern/jdk17/bin/jar', 'cf', str(root/'resident-mgsi.jar'), '-C', str(classes), '.'], check=True)


def main():
    assert not (ROOT/'preparation_complete.json').exists(), 'Already prepared.'
    for name in ('physical_index.with_blocks.json', 'golden_results.json'):
        shutil.copyfile(NCEP_STAGE/name, ROOT/name)
    meta = json.loads((ROOT/'physical_index.with_blocks.json').read_text())
    assert len(meta['files']) == 40 and len(meta['entries']) == 400
    compile_jar(ROOT)
    shutil.copyfile(ROOT/'physical_index.with_blocks.json', SQL_DIR/'ncep_meta.json')
    pins = {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
            ('ResidentMGSI.java', 'resident-mgsi.jar', 'rewrite_ncep.py', 'build_meta_ncep.py', 'physical_index.with_blocks.json')}
    (ROOT/'preparation_complete.json').write_text(json.dumps(
        {'status': 'READY', 'source': str(NCEP_STAGE), 'sha256': pins,
         'scope': 'P57(e): NCEP re-chunked variants plane/half/quarter/time12/time4; P61: NCEP full-archive point table',
         'new_hdfs_outputs': True, 'hdfs_output_dir': '/user/mgsi-experiments/chunk_size'}, indent=2))
    print('PREPARATION_READY[chunksize]', flush=True)


if __name__ == '__main__':
    main()
