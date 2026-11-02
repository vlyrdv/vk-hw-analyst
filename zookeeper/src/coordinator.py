import argparse
import csv
import gzip
import hashlib
import json
import os
import time
import urllib.request
import urllib.error
from collections import Counter
from core import ROOT, connect, encode, commit, log
from kazoo.exceptions import NodeExistsError, BadVersionError
import sys
import hw3_common

sys.modules["common"] = hw3_common
from hw3_mapreduce import mapper

p = argparse.ArgumentParser()
p.add_argument("--run", required=True)
p.add_argument("--name", required=True)
p.add_argument("--delay", type=float, default=0.04)
a = p.parse_args()
fencing = int(os.getenv("FENCING", "0"))
z = connect()
base = "/hw/" + a.run
log(
    "connected",
    coordinator=a.name,
    session_timeout_ms=z._session_timeout,
    fencing=fencing,
)
while True:
    _, stat = z.get(base + "/epoch")
    epoch = stat.version + 1
    tx = z.transaction()
    tx.create(
        base + "/leader", encode({"name": a.name, "pid": os.getpid()}), ephemeral=True
    )
    tx.set_data(base + "/epoch", encode(epoch), version=stat.version)
    try:
        commit(tx)
        epoch_version = epoch
        break
    except (NodeExistsError, BadVersionError):
        if json.loads(z.get(base + "/result")[0]):
            log("standby_finished")
            z.stop()
            z.close()
            sys.exit(0)
        time.sleep(0.1)
log("became_leader", coordinator=a.name, epoch=epoch, pid=os.getpid())
rows = list(
    csv.DictReader(
        (ROOT / "data/it_vacancies_2024_projection.csv").open(
            newline="", encoding="utf-8"
        )
    )
)
checkpoint = json.loads(z.get(base + "/checkpoint")[0])
counts = Counter()
start = matched = with_skills = 0
if checkpoint:
    raw = (ROOT / checkpoint["path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == checkpoint["sha256"]
    state = json.loads(gzip.decompress(raw))
    start, matched, with_skills = (
        state["next_row"],
        state["matched"],
        state["with_skills"],
    )
    counts = Counter({tuple(keys): n for keys, n in state["counts"]})
log("restored_checkpoint", next_row=start, unique_pairs=len(counts))
checkpoint_dir = ROOT / "checkpoints" / a.run
checkpoint_dir.mkdir(parents=True, exist_ok=True)
for offset in range(start, len(rows), 1000):
    mapped, m, s = mapper(rows[offset : offset + 1000])
    counts.update(mapped[1])
    matched += m
    with_skills += s
    end = min(offset + 1000, len(rows))
    if end % 10000 == 0 or end == len(rows):
        state = dict(
            next_row=end,
            matched=matched,
            with_skills=with_skills,
            counts=list(counts.items()),
        )
        raw = gzip.compress(encode(state), mtime=0)
        path = checkpoint_dir / f"{a.name}-e{epoch}-r{end}.json.gz"
        path.write_bytes(raw)
        tx = z.transaction()
        tx.check(base + "/epoch", version=epoch_version)
        tx.set_data(
            base + "/checkpoint",
            encode(
                {
                    "path": str(path.relative_to(ROOT)),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                }
            ),
        )
        try:
            commit(tx)
            log("checkpoint_saved", next_row=end, epoch=epoch)
        except BadVersionError:
            log("stale_checkpoint_rejected", next_row=end, epoch=epoch)
    time.sleep(a.delay)
out = ROOT / "results" / a.run
out.mkdir(parents=True, exist_ok=True)
path = out / f"{a.name}-e{epoch}-pairs.csv"
with path.open("w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["skill1", "skill2", "n"])
    for keys, n in sorted(counts.items()):
        w.writerow([*keys, n])
body = dict(
    coordinator=a.name,
    epoch=epoch,
    epoch_version=epoch_version,
    fencing=fencing,
    path=str(path.relative_to(ROOT)),
    sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
    unique_pairs=len(counts),
    total_pairs=sum(counts.values()),
    matched_vacancies=matched,
    vacancies_with_skills=with_skills,
)
log("calculation_finished", **body)
request = urllib.request.Request(
    "http://127.0.0.1:18089/result",
    data=encode(body),
    headers={"Content-Type": "application/json"},
)
try:
    with urllib.request.urlopen(request, timeout=20) as response:
        log("записал результат", **json.load(response), **body)
except urllib.error.HTTPError as e:
    if e.code != 409:
        raise
    log("BadVersionError", **json.load(e), coordinator=a.name, epoch=epoch)
finally:
    z.stop()
    z.close()
