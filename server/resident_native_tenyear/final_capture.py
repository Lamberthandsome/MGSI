"""Freeze completed runs and the cluster state before copying their evidence."""
import json
from pathlib import Path
import subprocess
import sys
import urllib.request

ROOT = Path(__file__).resolve().parent
for variant, expected in (("", 48), ("coalesced", 12), ("spatial_coalesced", 48), ("preinitialized", 48)):
    folder = ROOT / variant
    state = json.loads((folder / "state.json").read_text())
    rows = [json.loads(line) for line in (folder / "results.jsonl").read_text().splitlines()]
    assert state["status"] == "COMPLETE" and state["job_state"] == "FINISHED", (variant, state)
    assert len(rows) == expected and all(row["status"] == "PASS" for row in rows), variant
    print(json.dumps({"variant": variant or "native_base", "queries": len(rows), "initial_read_seconds": {
        row["region"]: row["until_final_120_month_results_seconds"] for row in rows if row["repeat"] == 0
    }}), flush=True)
subprocess.run([sys.executable, str(ROOT / "check_sources.py")], check=True)
with urllib.request.urlopen("http://192.0.2.64:8081/overview", timeout=20) as stream:
    overview = json.load(stream)
assert overview["taskmanagers"] == 24 and overview["slots-available"] == 72 and overview["jobs-running"] == 0, overview
(ROOT / "environment_final_all.json").write_text(json.dumps(overview, indent=2))
print(json.dumps({"cluster": overview}), flush=True)
subprocess.run([sys.executable, str(ROOT / "capture_evidence.py")], check=True)
