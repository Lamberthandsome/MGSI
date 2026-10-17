"""Stage the broadcast experiment (server): compile instrumented jar, stage metas."""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent
SOURCES = {
    'gleam': '/opt/mgsi-experiments/ablation_gleam/physical_index.with_blocks.json',
    'merra2': '/opt/mgsi-experiments/ablation_merra2/physical_index.with_blocks.json',
    'ncep': '/opt/mgsi-experiments/chunk_size/physical_index.with_blocks.json',
    'era5': '/opt/mgsi-experiments/matched_baselines_95pct/ecmwf_spatial/physical_index.with_blocks.json',
    'chirps': '/opt/mgsi-experiments/matched_baselines_95pct/chirps_spatial/physical_index.with_blocks.json',
}
WORKERS = ['192.0.2.65', '192.0.2.66', '192.0.2.71', '192.0.2.72']


def main():
    assert not (ROOT/'preparation_complete.json').exists(), 'Already prepared.'
    for name, source in SOURCES.items():
        target = ROOT/('meta_%s.json' % name)
        shutil.copyfile(source, target)
        md5 = subprocess.run(['md5sum', str(target)], check=True, stdout=subprocess.PIPE, universal_newlines=True).stdout.split()[0]
        for worker in WORKERS:
            subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', 'root@'+worker,
                            'mkdir -p '+str(ROOT)], check=True)
            subprocess.run(['scp', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', str(target),
                            'root@'+worker+':'+str(target)], check=True)
            remote = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=yes', 'root@'+worker,
                                     'md5sum '+str(target)], check=True, stdout=subprocess.PIPE, universal_newlines=True).stdout.split()[0]
            assert remote == md5, (worker, name)
    cp = ':'.join(['/opt/mgsi-cluster/flink/lib/*', '/app/hadoop-2.7.2/share/hadoop/common/*',
        '/app/hadoop-2.7.2/share/hadoop/common/lib/*', '/app/hadoop-2.7.2/share/hadoop/hdfs/*',
        '/app/hadoop-2.7.2/share/hadoop/hdfs/lib/*'])
    classes = ROOT/'classes_mgsi'
    if classes.exists():
        shutil.rmtree(classes)
    classes.mkdir()
    subprocess.run(['/opt/mgsi-modern/jdk17/bin/javac', '--release', '8', '-cp', cp, '-d', str(classes), str(ROOT/'ResidentMGSI.java')], check=True)
    base_jar = ROOT/'resident-mgsi.jar'
    subprocess.run(['/opt/mgsi-modern/jdk17/bin/jar', 'cf', str(base_jar), '-C', str(classes), '.'], check=True)
    # Embedded ('cached' mode) jars: the user jar itself carries the meta and is
    # shipped to every TaskManager via the blob transfer at job deploy.
    for name in SOURCES:
        embedded_jar = ROOT/('resident-mgsi-%s-embedded.jar' % name)
        shutil.copyfile(base_jar, embedded_jar)
        staged = ROOT/'meta.json'
        shutil.copyfile(ROOT/('meta_%s.json' % name), staged)
        subprocess.run(['/opt/mgsi-modern/jdk17/bin/jar', 'uf', str(embedded_jar), '-C', str(ROOT), 'meta.json'], check=True)
    pins = {name: hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in
            ('ResidentMGSI.java', 'resident-mgsi.jar', 'run_bcast.py')}
    sizes = {name: (ROOT/('meta_%s.json' % name)).stat().st_size for name in SOURCES}
    (ROOT/'preparation_complete.json').write_text(json.dumps(
        {'status': 'READY', 'sha256': pins, 'meta_bytes': sizes,
         'scope': 'P78: distributed-cache broadcast vs pre-staged, per-JVM load ms and heap delta, five metas'}, indent=2))
    print('PREPARATION_READY[broadcast] '+json.dumps(sizes), flush=True)


if __name__ == '__main__':
    main()
