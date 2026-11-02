import argparse
import csv
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from core import ROOT, connect, encode, log

p = argparse.ArgumentParser()
p.add_argument("--fencing", type=int, choices=(0, 1), required=True)
p.add_argument("--run", required=True)
a = p.parse_args()
folder = ROOT / "logs" / a.run
folder.mkdir(parents=True, exist_ok=False)
z = connect()
base = "/hw/" + a.run
assert not z.exists(base), "Use a fresh run name"
z.ensure_path(base)
for key, data in [("epoch", 0), ("checkpoint", {}), ("result", {})]:
    z.create(base + "/" + key, encode(data))
processes = []
handles = []


def launch(script, name, extra=()):
    f = (folder / (name + ".log")).open("w")
    handles.append(f)
    env = dict(os.environ, FENCING=str(a.fencing), PYTHONUNBUFFERED="1")
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "src" / script), "--run", a.run, *extra],
        stdout=f,
        stderr=subprocess.STDOUT,
        env=env,
    )
    processes.append(proc)
    return proc


def events(name):
    result = []
    for line in (folder / (name + ".log")).read_text().splitlines():
        try:
            result.append(json.loads(line))
        except json.JSONDecodeError:
            pass
    return result


def until(test, seconds=90):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        value = test()
        if value:
            return value
        time.sleep(0.05)
    raise TimeoutError("See coordinator logs: " + str(folder))


leader = None
try:
    receiver = launch("receiver.py", "receiver")
    until(lambda: any(e["event"] == "receiver_ready" for e in events("receiver")))
    leader = launch("coordinator.py", "primary", ("--name", "primary"))
    until(lambda: any(e["event"] == "became_leader" for e in events("primary")))
    backup = launch("coordinator.py", "backup", ("--name", "backup"))
    until(
        lambda: any(
            e["event"] == "checkpoint_saved" and e["next_row"] >= 30000
            for e in events("primary")
        )
    )
    os.kill(leader.pid, signal.SIGSTOP)
    paused = time.monotonic()
    log("SIGSTOP", pid=leader.pid, monotonic=paused)
    elected = until(
        lambda: next(
            (e for e in events("backup") if e["event"] == "became_leader"), None
        )
    )
    elapsed = elected["mono"] - paused
    log("failover_measured", seconds=elapsed)
    backup.wait(timeout=90)
    assert backup.returncode == 0
    assert any(e["event"] == "записал результат" for e in events("backup"))
    remaining = 40 - (time.monotonic() - paused)
    if remaining > 0:
        time.sleep(remaining)
    os.kill(leader.pid, signal.SIGCONT)
    resumed = time.monotonic()
    log(
        "SIGCONT",
        pid=leader.pid,
        suspended_seconds=resumed - paused,
        backup_already_finished=True,
    )
    leader.wait(timeout=120)
    assert leader.returncode == 0
    pointer, stat = z.get(base + "/result")
    pointer = json.loads(pointer)
    accepted = [
        json.loads(s) for s in (folder / "accepted.jsonl").read_text().splitlines()
    ]
    expected = 1 if a.fencing else 2
    assert len(accepted) == expected and stat.version == expected
    assert pointer["coordinator"] == ("backup" if a.fencing else "primary")
    assert any(
        e["event"] == ("BadVersionError" if a.fencing else "записал результат")
        for e in events("primary")
    )
    restored = next(e for e in events("backup") if e["event"] == "restored_checkpoint")
    assert restored["next_row"] > 0
    reference = list(csv.reader((ROOT / "data/hw3_k2.csv").open(newline="")))
    comparisons = []
    for name in ("primary", "backup"):
        e = next(e for e in events(name) if e["event"] == "calculation_finished")
        actual = list(csv.reader((ROOT / e["path"]).open(newline="")))
        assert actual == reference, name + " differs from HW3"
        assert e["unique_pairs"] == 96052 and e["total_pairs"] == 506354
        comparisons.append(
            dict(
                coordinator=name,
                all_csv_rows_equal=True,
                unique_pairs=e["unique_pairs"],
                total_pairs=e["total_pairs"],
                sha256=e["sha256"],
            )
        )
    summary = dict(
        run=a.run,
        fencing=a.fencing,
        session_timeout_ms=10000,
        suspended_seconds=resumed - paused,
        failover_seconds=elapsed,
        checkpoint_next_row=restored["next_row"],
        accepted_count=len(accepted),
        pointer_version=stat.version,
        pointer=pointer,
        comparisons=comparisons,
    )
    (ROOT / "results" / a.run / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    )
    log("experiment_passed", **summary)
finally:
    if leader and leader.poll() is None:
        os.kill(leader.pid, signal.SIGCONT)
    for proc in reversed(processes):
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
    for h in handles:
        h.close()
    z.stop()
    z.close()
