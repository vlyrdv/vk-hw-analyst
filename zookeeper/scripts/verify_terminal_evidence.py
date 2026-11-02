import csv
import gzip
import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
manifest = json.loads((ROOT / "results/terminal-evidence.json").read_text())


def events(path):
    result = []
    for line in path.read_text().splitlines():
        try:
            value = json.loads(line)
            if isinstance(value, dict):
                result.append(value)
        except ValueError:
            pass
    return result


ensemble = ROOT / "logs" / manifest["ensemble_session"]
for i, role in [(1, "follower"), (2, "follower"), (3, "leader")]:
    assert "Mode: " + role in (ensemble / f"zk{i}-status.txt").read_text()
assert json.loads((ensemble / "quorum-write-read.txt").read_text())["success"]
assert "not currently serving requests" in (ensemble / "no-quorum-srvr.txt").read_text()
assert "KazooTimeoutError" in (ensemble / "no-quorum-client.txt").read_text()
session = ROOT / "logs" / manifest["session"]
creation = (session / "ephemeral-creator-full.txt").read_text()
created = re.search(r"Created (/terminal-ephemeral-\S+)", creation).group(1)
present = (session / "ephemeral-get-present.txt").read_text()
owner = re.search(r"ephemeralOwner = (0x[0-9a-fA-F]+)", present).group(1)
assert int(owner, 16) != 0 and "session-owned" in present
assert (
    "Node does not exist: " + created
    in (session / "ephemeral-get-deleted.txt").read_text()
)
reference = next(
    item["sha256"]
    for item in json.loads((ROOT / "results/source-manifest.json").read_text())["files"]
    if item["key"].endswith("mapreduce_k2.csv")
)
for fencing, run in enumerate(manifest["runs"]):
    summary = json.loads((ROOT / "results" / run / "summary.json").read_text())
    primary = events(ROOT / "logs" / run / "primary.log")
    backup = events(ROOT / "logs" / run / "backup.log")
    accepted = events(ROOT / "logs" / run / "accepted.jsonl")
    control = events(session / f"fencing-{fencing}-controller.log")
    stop = next(e for e in control if e["event"] == "SIGSTOP")
    resume = next(e for e in control if e["event"] == "SIGCONT")
    leader = next(e for e in backup if e["event"] == "became_leader")
    assert abs(leader["mono"] - stop["monotonic"] - summary["failover_seconds"]) < 0.001
    assert summary["session_timeout_ms"] == 10000 and summary["suspended_seconds"] >= 40
    assert abs(resume["suspended_seconds"] - summary["suspended_seconds"]) < 0.001
    assert (
        summary["accepted_count"]
        == summary["pointer_version"]
        == (2 if not fencing else 1)
    )
    assert len(accepted) == summary["accepted_count"]
    assert summary["pointer"]["coordinator"] == ("primary" if not fencing else "backup")
    assert summary["pointer"]["epoch"] == (1 if not fencing else 2)
    assert sum(e["event"] == "записал результат" for e in primary + backup) == len(
        accepted
    )
    assert any(
        e["event"] == ("записал результат" if not fencing else "BadVersionError")
        for e in primary
    )
    recovered = next(e for e in backup if e["event"] == "restored_checkpoint")
    assert recovered["next_row"] == 30000 and recovered["unique_pairs"] == 23277
    assert (
        next(e for e in backup if e["event"] == "записал результат")["mono"]
        < resume["mono"]
    )
    state = json.loads(
        gzip.decompress(
            (
                ROOT
                / "results"
                / "checkpoint-evidence"
                / f"terminal-fencing-{fencing}-r30000.json.gz"
            ).read_bytes()
        )
    )
    assert state["next_row"] == 30000 and len(state["counts"]) == 23277
    for who, log in [("primary", primary), ("backup", backup)]:
        result = next(e for e in log if e["event"] == "calculation_finished")
        data = (ROOT / result["path"]).read_bytes()
        assert hashlib.sha256(data).hexdigest() == reference == result["sha256"]
        with (ROOT / result["path"]).open(newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 96052 and sum(int(row["n"]) for row in rows) == 506354
    print(
        f'{run}: PASS; failover={summary["failover_seconds"]:.3f}s; '
        f'accepted={len(accepted)}; version={summary["pointer_version"]}; '
        "all CSVs equal HW3"
    )
assert len(manifest["screenshots"]) == 8
for item in manifest["screenshots"]:
    assert (
        hashlib.sha256((ROOT / "screenshots" / item["file"]).read_bytes()).hexdigest()
        == item["sha256"]
    )
print(
    "PASS: roles, quorum, ephemeral session, checkpoint recovery, "
    "real pause, timings, unsafe duplicate, fencing rejection, "
    "full CSV equality and all 8 original terminal screenshots."
)
