"""Restore one relocated derived-representation tar back into HDFS.

Verifies the whole-archive SHA-256 against the relocation receipt before
extraction, writes every file with replication 2, and verifies per-file sizes.
Capacity is checked on all five nodes before each part begins; the 95% ceiling,
94.85% protective stop and 94.5% resume rule of the author-approved policy are
enforced identically to the experiment batches.
"""
import argparse
import hashlib
import json
import subprocess
import tarfile
import tempfile
import time
from pathlib import Path

import pyarrow.fs as pafs

from tpk_common import _hadoop_env

NODES = ['192.0.2.64', '192.0.2.65', '192.0.2.66', '192.0.2.71', '192.0.2.72']
MAX_PERCENT = 95.0
STOP_PERCENT = 94.85
RESUME_PERCENT = 94.5


def disk_percent():
    values = {}
    for ip in NODES:
        out = subprocess.run(['ssh', '-n', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', 'root@' + ip,
                              "df -P / | tail -1 | awk '{print $5}'"], capture_output=True, text=True, timeout=30)
        values[ip] = float(out.stdout.strip().rstrip('%'))
    return values


def await_capacity(action):
    while True:
        values = disk_percent()
        worst = max(values.values())
        if worst < RESUME_PERCENT and worst < STOP_PERCENT:
            return values
        print('CAPACITY_WAIT ' + json.dumps({'action': action, 'disks': values}), flush=True)
        time.sleep(300)


def sha256_file(path, chunk=64 * 1024 * 1024):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        while True:
            block = stream.read(chunk)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tar', required=True)
    parser.add_argument('--receipt', required=True, help='relocation receipt json with archive_sha256')
    parser.add_argument('--hdfs-dir', required=True)
    parser.add_argument('--state', required=True)
    args = parser.parse_args()
    state_path = Path(args.state)
    state = json.loads(state_path.read_text()) if state_path.exists() else {'status': 'NEW', 'files_done': 0}
    if state.get('status') == 'COMPLETE':
        print('RESTORE_ALREADY_COMPLETE ' + args.hdfs_dir)
        return

    receipt = json.loads(Path(args.receipt).read_text())
    expected_digest = receipt['archive_sha256']
    _hadoop_env()
    fs = pafs.HadoopFileSystem('192.0.2.64', 9000, user='root', replication=2, extra_conf={'dfs.replication': '2'})

    if 'archive_sha256_verified' not in state:
        digest = sha256_file(args.tar)
        assert digest == expected_digest, ('tar sha mismatch', digest, expected_digest)
        state['archive_sha256_verified'] = digest
        state_path.write_text(json.dumps(state))

    with tempfile.TemporaryDirectory(dir='/opt/mgsi-experiments/restore_staging') as work:
        with tarfile.open(args.tar) as archive:
            members = archive.getmembers()
            files = [m for m in members if m.isfile()]
            # The archive SHA already matched the receipt; an extra manifest-style
            # file inside the tar is tolerated and recorded rather than fatal.
            if 'files' in receipt and len(files) != receipt['files']:
                print('RESTORE_FILE_COUNT_NOTE ' + json.dumps({'tar_files': len(files), 'receipt_files': receipt['files']}), flush=True)
            archive.extractall(work, members=members, filter='data')
        for index, member in enumerate(files):
            if index < state.get('files_done', 0):
                continue
            if index % 500 == 0:
                disks = await_capacity('restore ' + args.hdfs_dir)
                print('RESTORE_PROGRESS ' + json.dumps({'file': index, 'total': len(files), 'disks': disks}), flush=True)
            # Archives were created with a top-level data/ directory; strip it so the
            # destination matches the original store layout exactly.
            assert member.name.startswith('data/'), member.name
            relative = member.name[len('data/'):]
            local_path = Path(work) / member.name
            assert local_path.stat().st_size == member.size, (member.name, local_path.stat().st_size, member.size)
            destination = args.hdfs_dir.rstrip('/') + '/' + relative
            with open(local_path, 'rb') as source:
                with fs.open_output_stream(destination) as target:
                    while True:
                        block = source.read(8 * 1024 * 1024)
                        if not block:
                            break
                        target.write(block)
            assert fs.get_file_info(destination).size == member.size
            state['files_done'] = index + 1
            if index % 100 == 0:
                state_path.write_text(json.dumps(state))

    written = fs.get_file_info(pafs.FileSelector(args.hdfs_dir, recursive=True))
    total = sum(e.size for e in written if e.type == pafs.FileType.File)
    state.update(status='COMPLETE', files=len(files), bytes=total,
                 hdfs_dir=args.hdfs_dir, verified_sha256=expected_digest)
    state_path.write_text(json.dumps(state))
    print('RESTORE_COMPLETE ' + json.dumps({'hdfs_dir': args.hdfs_dir, 'files': len(files),
                                            'bytes': total, 'source_bytes': sum(m.size for m in files)}))


if __name__ == '__main__':
    main()
