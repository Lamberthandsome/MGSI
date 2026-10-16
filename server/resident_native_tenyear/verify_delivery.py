"""Read-only verification of uploaded reports and the final cluster state."""
import hashlib
import json
from pathlib import Path
import urllib.request

root = Path('/opt/mgsi-experiments/20261006-resident-native')
manifest = json.loads((root / 'delivery_manifest.json').read_text(encoding='utf-8'))
for name, expected in manifest.items():
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected, name
with urllib.request.urlopen('http://192.0.2.64:8081/overview', timeout=20) as stream:
    environment = json.load(stream)
assert environment['taskmanagers'] == 24 and environment['slots-available'] == 72 and environment['jobs-running'] == 0, environment
print(json.dumps({'status': 'PASS', 'uploaded_files_checked': len(manifest), 'cluster': environment}))
