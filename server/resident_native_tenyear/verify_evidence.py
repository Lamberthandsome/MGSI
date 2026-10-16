"""Safely extract a capsule and check every file before deriving results."""
import hashlib
import json
from pathlib import Path,PurePosixPath
import sys
import tarfile

ROOT=Path(__file__).resolve().parent
archive=Path(sys.argv[1])
target=(ROOT/sys.argv[2]).resolve()
assert target.is_relative_to(ROOT) and target!=ROOT
target.mkdir(parents=True,exist_ok=True)
with tarfile.open(archive,'r:gz') as bundle:
    for member in bundle.getmembers():
        relative=PurePosixPath(member.name)
        assert member.isfile() and not relative.is_absolute() and '..' not in relative.parts
        path=target.joinpath(*relative.parts)
        assert path.resolve().is_relative_to(target)
        path.parent.mkdir(parents=True,exist_ok=True)
        with bundle.extractfile(member) as stream:
            path.write_bytes(stream.read())
manifest=json.loads((target/'snapshot_manifest.json').read_text())
for row in manifest['files']:
    path=target/row['path']
    assert path.stat().st_size==row['bytes'] and hashlib.sha256(path.read_bytes()).hexdigest()==row['sha256'],row['path']
assert (target/'results.jsonl').is_file(),'Complete native records missing from snapshot.'
print(json.dumps({'status':'PASS','files_verified':len(manifest['files']),'time':manifest['time'],'target':str(target)},ensure_ascii=False))
