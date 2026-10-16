"""Start exactly this authorized, independently guarded batch, not legacy queues."""
from pathlib import Path
import hashlib
import json
import os
import subprocess
import time
from guard import check

ROOT=Path(__file__).resolve().parent

def main():
    assert not (ROOT/'controller.json').exists()
    check(running=False)
    authorization={'matched_baselines_authorized':True,'disk_max_percent':90,
        'instruction':'2026-10-11: 新基线和MGSI一起做；每节点磁盘尽量不超过90%；准备好后直接开启。',
        'scope':'MGSI + Sedona + original-file xarray/Dask + Zarr/Dask; first GLEAM spatial batch',
        'legacy_auto_resume':False,'dynamic_load_and_failure_injection_authorized':False}
    (ROOT/'authorization.json').write_text(json.dumps(authorization,ensure_ascii=False,indent=2))
    files=[p for p in ROOT.iterdir() if p.suffix in ('.py','.java')]
    (ROOT/'code_pin.json').write_text(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in files},indent=2))
    env=os.environ.copy(); env.update(PYTHONDONTWRITEBYTECODE='1',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1',MKL_NUM_THREADS='1',
        LIBHDFS_OPTS='-Xmx256m -XX:ActiveProcessorCount=3',PYTHONPATH=str(ROOT))
    with (ROOT/'controller.log').open('x') as stream:
        p=subprocess.Popen(['/opt/mgsi-modern/python312/bin/python','-u',str(ROOT/'controlled_pipeline.py')],
            cwd=str(ROOT),env=env,stdout=stream,stderr=subprocess.STDOUT,start_new_session=True)
    ticks=(Path('/proc')/str(p.pid)/'stat').read_text().split()[21]
    (ROOT/'controller.json').write_text(json.dumps({'pid':p.pid,'start_ticks':ticks,'root':str(ROOT)},indent=2))
    time.sleep(2); assert p.poll() is None, 'Controller stopped immediately; inspect retained log.'
    print(json.dumps({'status':'STARTED','pid':p.pid,'root':str(ROOT),'disk_start_below':89.8,'disk_stop_at':89.85}),flush=True)

if __name__=='__main__': main()
