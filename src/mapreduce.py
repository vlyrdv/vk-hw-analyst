import argparse
import csv
import json
from collections import Counter
from itertools import combinations, islice
from pathlib import Path
from common import BUCKET, ROLE, Meter, rows_from_s3, s3, skills


def mapper(chunk):
    out = [Counter(), Counter(), Counter()]
    matched = 0
    with_skills = 0
    for row in chunk:
        if row["role"] != ROLE:
            continue
        matched += 1
        ss = skills(row["key_skills"])
        with_skills += bool(ss)
        for k in (1, 2, 3):
            out[k - 1].update(combinations(ss, k))
    return out, matched, with_skills


def run(chunk_size=1000, inject_failure_chunk=0):
    total = [Counter(), Counter(), Counter()]
    matched = has_skills = chunks = retries = 0
    it = iter(rows_from_s3())
    while chunk := list(islice(it, chunk_size)):
        chunks += 1
        for attempt in range(2):
            try:
                if chunks == inject_failure_chunk and attempt == 0:
                    raise RuntimeError("injected mapper failure")
                mapped, m, s = mapper(chunk)
                break
            except Exception:
                if attempt:
                    raise
                retries += 1
        matched += m
        has_skills += s
        for t, part in zip(total, mapped):
            t.update(part)
    return total, {
        "matched_vacancies": matched,
        "vacancies_with_skills": has_skills,
        "chunks": chunks,
        "mapper_retries": retries,
    }


def save(counters, outdir, upload=True):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    client = s3() if upload else None
    for k, c in enumerate(counters, 1):
        p = outdir / f"mapreduce_k{k}.csv"
        with p.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow([f"skill{i}" for i in range(1, k + 1)] + ["n"])
            for combo, n in sorted(c.items()):
                w.writerow([*combo, n])
        if client:
            client.upload_file(str(p), BUCKET, f"results/analyst/mapreduce_k{k}.csv")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--chunk-size", type=int, default=1000)
    p.add_argument("--outdir", default="results")
    p.add_argument("--no-upload", action="store_true")
    p.add_argument("--inject-failure-chunk", type=int, default=0)
    a = p.parse_args()
    with Meter() as meter:
        counts, meta = run(a.chunk_size, a.inject_failure_chunk)
    save(counts, a.outdir, not a.no_upload)
    metrics = {
        "method": "mapreduce",
        "seconds": round(meter.elapsed, 3),
        "peak_rss_mb": round(meter.peak / 1048576, 2),
        "chunk_size": a.chunk_size,
        **meta,
        **{f"k{k}_unique": len(c) for k, c in enumerate(counts, 1)},
        **{f"k{k}_total": sum(c.values()) for k, c in enumerate(counts, 1)},
    }
    print(json.dumps(metrics, ensure_ascii=False))


if __name__ == "__main__":
    main()
