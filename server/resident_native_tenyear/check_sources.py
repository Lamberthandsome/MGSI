"""Check original source identities and generated SQL file lengths after runs."""
import json
from pathlib import Path
from hdfs import InsecureClient

ROOT=Path(__file__).resolve().parent
meta=json.loads((ROOT/'physical_index.with_blocks.json').read_text())
source=json.loads((ROOT/'sql_input_manifest.json').read_text())
client=InsecureClient('http://192.0.2.64:50070',user='root')
checked=[]
for file in meta['files']:
    status=client.status(file['path'])
    assert (status['length'],status['modificationTime'])==(file['source_bytes'],file['source_mtime']),file['path']
    checked.append({'path':file['path'],'bytes':status['length'],'mtime':status['modificationTime']})
for file in source['files']:
    assert client.status(file['path'])['length']==file['bytes'],file['path']
result={'status':'PASS','original_sources':checked,'parquet_file_lengths_checked':len(source['files']),
        'scope':'HDFS source length/mtime and Parquet lengths; not complete contents hashes.'}
(ROOT/'source_immutability.json').write_text(json.dumps(result,indent=2))
print(json.dumps({k:v for k,v in result.items() if k!='original_sources'}),flush=True)
