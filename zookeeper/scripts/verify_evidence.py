import csv
import gzip
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def events(path):
    out = []
    for line in path.read_text().splitlines():
        try:
            value = json.loads(line)
            if isinstance(value, dict):
                out.append(value)
        except json.JSONDecodeError:
            pass
    return out


ensemble = events(ROOT / "logs/ensemble.txt")
assert any(e["event"] == "ensemble_checks_passed" for e in ensemble)
assert any(e["event"] == "two_servers_write_read" and e["success"] for e in ensemble)
assert any(
    e["event"] == "one_server_no_quorum" and not e["connected"] for e in ensemble
)
assert any(e["event"] == "ephemeral_deleted_after_cli_close" for e in ensemble)
text = (ROOT / "logs/ensemble.txt").read_text()
assert "ephemeralOwner = 0x" in text and "Node does not exist: /hw-ephemeral" in text
hashes = []
for name, fencing in [("without-fencing", 0), ("with-fencing", 1)]:
    summary = json.loads((ROOT / "results" / name / "summary.json").read_text())
    logdir = ROOT / "logs" / name
    primary = events(logdir / "primary.log")
    backup = events(logdir / "backup.log")
    controller = events(ROOT / "logs" / f"{name}-controller.log")
    pause = next(e for e in controller if e["event"] == "SIGSTOP")
    resume = next(e for e in controller if e["event"] == "SIGCONT")
    elected = next(e for e in backup if e["event"] == "became_leader")
    assert (
        abs((elected["mono"] - pause["monotonic"]) - summary["failover_seconds"])
        < 0.001
    )
    assert elected["mono"] > pause["monotonic"]
    assert resume["suspended_seconds"] >= 40
    accepted = events(logdir / "accepted.jsonl")
    assert len(accepted) == (1 if fencing else 2)
    assert summary["pointer_version"] == len(accepted)
    assert summary["pointer"]["coordinator"] == ("backup" if fencing else "primary")
    written = next(e for e in backup if e["event"] == "записал результат")
    assert written["mono"] < resume["mono"]
    assert any(
        e["event"] == "restored_checkpoint" and e["next_row"] == 30000 for e in backup
    )
    checkpoint = json.loads(
        gzip.decompress(
            (
                ROOT / "results/checkpoint-evidence" / f"{name}-r30000.json.gz"
            ).read_bytes()
        )
    )
    assert checkpoint["next_row"] == 30000 and len(checkpoint["counts"]) == 23277
    assert sum(e["event"] == "записал результат" for e in primary + backup) == (
        1 if fencing else 2
    )
    assert any(
        e["event"] == ("BadVersionError" if fencing else "записал результат")
        for e in primary
    )
    for who, logs in [("primary", primary), ("backup", backup)]:
        result = next(e for e in logs if e["event"] == "calculation_finished")
        path = ROOT / result["path"]
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        assert digest == result["sha256"]
        with path.open(newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 96052 and sum(int(row["n"]) for row in rows) == 506354
        assert (
            result["matched_vacancies"] == 30112
            and result["vacancies_with_skills"] == 17932
        )
        hashes.append(digest)
        print(f"{name}/{who}: 96052 unique pairs, 506354 occurrences, SHA-256 {digest}")
manifest = json.loads((ROOT / "results/source-manifest.json").read_text())
reference = next(
    f["sha256"] for f in manifest["files"] if f["key"].endswith("mapreduce_k2.csv")
)
assert len(set(hashes)) == 1 and hashes[0] == reference
print(
    "PASS: ensemble, ephemeral node, checkpoint recovery, "
    "monotonic failover timings, unsafe duplicate, epoch rejection, "
    "four CSVs byte-identical to HW3."
)
