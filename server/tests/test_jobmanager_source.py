"""Exercise the real Flink 1.11.3 source with a local MiniCluster and metadata fixtures.

No climate files, HDFS reads, performance tables or failure injection are used.
Pass --classpath or --classpath-json with Flink/Hadoop dependencies.
"""
from pathlib import Path
import argparse
import importlib.util
import json
import os
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--java', default='java')
    parser.add_argument('--javac', default='javac')
    paths = parser.add_mutually_exclusive_group(required=True)
    paths.add_argument('--classpath')
    paths.add_argument('--classpath-json', type=Path)
    parser.add_argument('--classes-dir', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    args = parser.parse_args()
    server = Path(__file__).resolve().parents[1]
    classes = args.classes_dir.resolve()
    classes.mkdir(parents=True, exist_ok=True)
    report_path = args.report.resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    if args.classpath_json:
        dependencies = json.loads(args.classpath_json.read_text(encoding='utf-8'))
        classpath = os.pathsep.join(['.'] + [path for path in dependencies if path.endswith('.jar')])
    else:
        classpath = '.' + os.pathsep + args.classpath
    sources = [server/'JobManagerGlobalIndexSource.java', server/'ResidentMGSI.java', server/'tests/JobManagerGlobalIndexSourceCheck.java']
    compiled = subprocess.run([args.javac, '-encoding', 'UTF-8', '-source', '8', '-target', '8', '-cp', classpath, '-d', '.'] + [os.path.relpath(path, classes) for path in sources], cwd=classes, capture_output=True)
    if compiled.returncode:
        raise RuntimeError(compiled.stderr.decode(errors='replace'))
    spec = importlib.util.spec_from_file_location('mgsi_resident_driver', server/'run_resident.py')
    driver = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)
    connections = driver.Connections('127.0.0.1', 0, routes=2)
    port = connections.server.getsockname()[1]
    entries = []
    for row in range(3):
        for column in range(4):
            entries.append({'entry_id': len(entries), 'lon_min': column, 'lon_max': column+1,
                            'lat_min': row, 'lat_max': row+1})
    entries += [
        {'entry_id': 20, 'lon_min': 0, 'lon_max': 5, 'lat_min': 0, 'lat_max': 5},
        {'entry_id': 21, 'lon_min': 0, 'lon_max': 0, 'lat_min': 0, 'lat_max': 0},
        {'entry_id': 30, 'lon_min': -5, 'lon_max': -1, 'lat_min': -5, 'lat_max': -1},
    ]
    cases = {
        'outside': [100, 101, 100, 101],
        'boundary': [1, 1, 1, 1],
        'complete': [-90, 90, -180, 180],
        'partial': [-.5, 1.5, .5, 2.5],
        'representative_outside': [3.5, 4, 3.5, 4],
        'same_coordinate': [0, 0, 0, 0],
        'negative': [-2, -1, -2, -1],
    }
    ownership = {entry['entry_id']: i % 2 for i, entry in enumerate(entries)}
    results = []
    log_path = report_path.with_suffix('.log')
    process = None
    with tempfile.TemporaryDirectory(prefix='mgsi-jm-source-') as temporary:
        metadata = Path(temporary)/'metadata.json'
        metadata.write_text(json.dumps({'entries': entries}), encoding='utf-8')
        with log_path.open('wb') as log:
            try:
                process = subprocess.Popen([args.java, '-Xmx512m', '-cp', classpath,
                                           'JobManagerGlobalIndexSourceCheck', '127.0.0.1', str(port), str(metadata)],
                                          cwd=classes, stdout=log, stderr=subprocess.STDOUT)
                deadline = time.monotonic() + 45
                while connections.source is None or len(connections.workers) < 2:
                    if process.poll() is not None:
                        raise RuntimeError('Flink check stopped during initialization. See '+str(log_path))
                    if time.monotonic() > deadline:
                        raise TimeoutError('Flink source or sink readiness timeout. See '+str(log_path))
                    time.sleep(.05)
                hello = connections.source_info
                assert hello['global_index_component'] == 'SourceCoordinator/SplitEnumerator', hello
                assert 'SourceCoordinator' in hello['coordinator_thread'], hello
                assert int(hello['coordinator_jvm'].split('@')[0]) != os.getpid(), hello
                connections.configure_ownership(ownership)
                for name, box in cases.items():
                    connections.send([{'type': 'query', 'query_id': name, 'box': box, 'clear_epoch': name}])
                    rows = connections.results(name)
                    plan = connections.query_plan(name)
                    expected = {entry['entry_id'] for entry in entries
                                if entry['lat_max'] >= box[0] and entry['lat_min'] <= box[1]
                                and entry['lon_max'] >= box[2] and entry['lon_min'] <= box[3]}
                    assert set(plan['selected_entries']) == expected, (name, plan, expected)
                    actual = [entry for row in rows for entry in row['entries']]
                    assert len(actual) == len(set(actual)) and set(actual) == expected, (name, rows)
                    assert all(ownership[entry] == row['route'] for row in rows for entry in row['entries']), rows
                    assert all(row['clear_epoch'] == name and row['box'] == box for row in rows), rows
                    assert plan['coordinator_jvm'] == hello['coordinator_jvm'], plan
                    assert 'SourceCoordinator' in plan['coordinator_thread'], plan
                    results.append({'case': name, 'selected': sorted(actual), 'coordinator_thread': plan['coordinator_thread']})
                connections.send([{'type': 'stop'}])
                assert process.wait(timeout=30) == 0, 'Flink check failed. See '+str(log_path)
            finally:
                connections.server.close()
                if process is not None and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=10)
    log_text = log_path.read_text(encoding='utf-8', errors='replace')
    assert 'MGSI_SOURCE_UNIT_PASS' in log_text and 'MGSI_SOURCE_INTEGRATION_PASS' in log_text
    report = {'all_passed': True, 'flink_version': '1.11.3', 'mode': 'local MiniCluster, source plus two echo sinks',
              'global_index_hello': hello, 'cases': results, 'unit_checks': 'serializer versions, lexical pivot ties, complete bounds, boundaries, NaN rejection, 600-node skewed tree',
              'performance_experiment': False, 'corrupted_index_or_TaskManager_fault_test': False}
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'all_passed': True, 'query_cases': len(results), 'coordinator_thread': hello['coordinator_thread'],
                      'coordinator_jvm': hello['coordinator_jvm']}, ensure_ascii=False))


if __name__ == '__main__':
    main()
