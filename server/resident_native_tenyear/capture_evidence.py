"""Snapshot only this experiment, including its complete JSONL query records."""
import datetime
import hashlib
import io
import json
from pathlib import Path
import tarfile

ROOT=Path(__file__).resolve().parent
target=ROOT/('evidence_'+datetime.datetime.now().strftime('%Y%m%dT%H%M%S')+'.tar.gz')
selected=sorted(p for p in ROOT.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p.suffix in ('.py','.java','.jar','.json','.jsonl','.log','.md','.csv') and p.stat().st_size<80*1024**2)
manifest={'time':datetime.datetime.now().isoformat(),'files':[]}
with tarfile.open(str(target),'w:gz') as archive:
    for path in selected:
        content=path.read_bytes()
        if path.suffix=='.jsonl':
            for line in content.splitlines():
                json.loads(line)
        info=tarfile.TarInfo(path.relative_to(ROOT).as_posix())
        info.size=len(content)
        archive.addfile(info,io.BytesIO(content))
        manifest['files'].append({'path':info.name,'bytes':len(content),'sha256':hashlib.sha256(content).hexdigest()})
    content=(json.dumps(manifest,indent=2)+'\n').encode()
    info=tarfile.TarInfo('snapshot_manifest.json')
    info.size=len(content)
    archive.addfile(info,io.BytesIO(content))
print(json.dumps({'archive':str(target),'bytes':target.stat().st_size,'files':len(selected),'main_result_bytes':(ROOT/'results.jsonl').stat().st_size}),flush=True)
