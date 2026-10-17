"""Server-side restore queue: wait for uploaded tars, verify, write back to HDFS.

Uploads arrive in /opt/mgsi-experiments/restore_staging with a matching
<name>.upload_done marker placed by the local uploader after scp completes.
Each part restores under the destination recorded in its relocation receipt
key; per-part state lands in restore_states/<name>.json, which tpk_pipeline
waits for before using a dataset. Multiple shard workers may run in parallel;
each takes an interleaved subset so every part has exactly one owner.
"""
import argparse
import json
import subprocess
import time
from pathlib import Path

STAGING = Path('/opt/mgsi-experiments/restore_staging')
ROOT = Path('/opt/mgsi-experiments/tpk')
STATES = ROOT / 'restore_states'
PYTHON = '/opt/mgsi-modern/python312/bin/python'

GLEAM_ZARR_BASE = '/mgsi-experiments/20261007-modern-matrix/prepare_full/zarr_gleam_all_time'
GLEAM_SEDONA_BASE = '/mgsi-experiments/20261007-modern-matrix/prepare_full/sedona_gleam_all_time'
ECMWF_SEDONA_BASE = '/mgsi-experiments/20261011-matched-95/ecmwf_full_hdf/sedona_all_time'

PARTS = [('gleam_zarr_{}'.format(y), GLEAM_ZARR_BASE) for y in range(1981, 1991)]
PARTS += [('gleam_sedona_{}'.format(y), GLEAM_SEDONA_BASE) for y in range(1981, 1991)]
PARTS += [('ecmwf_sedona_{}'.format(y), ECMWF_SEDONA_BASE) for y in range(1980, 1990)]


def wait_upload(name):
    while not (STAGING / (name + '.upload_done')).exists():
        time.sleep(60)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--shard', type=int, default=0)
    parser.add_argument('--shards', type=int, default=1)
    args = parser.parse_args()
    STATES.mkdir(exist_ok=True)
    log = (ROOT / 'restore_queue_s{}.log'.format(args.shard)).open('a')
    for name, destination in PARTS[args.shard::args.shards]:
        state_path = STATES / (name + '.json')
        if state_path.exists() and json.loads(state_path.read_text()).get('status') == 'COMPLETE':
            print('SKIP_COMPLETE ' + name, file=log, flush=True)
            continue
        wait_upload(name)
        print('RESTORE_START ' + name + ' -> ' + destination, file=log, flush=True)
        result = subprocess.run([PYTHON, str(ROOT / 'restore_store.py'), '--tar', str(STAGING / (name + '.tar')),
                                 '--receipt', str(STAGING / (name + '.relocation.json')),
                                 '--hdfs-dir', destination, '--state', str(state_path)],
                                stdout=log, stderr=subprocess.STDOUT)
        if result.returncode:
            print('RESTORE_FAILED ' + name, file=log, flush=True)
            raise SystemExit('restore failed: ' + name)
        print('RESTORE_DONE ' + name, file=log, flush=True)
    print('RESTORE_QUEUE_COMPLETE_S{}'.format(args.shard), file=log, flush=True)


if __name__ == '__main__':
    main()
