import csv, json, sys
from pathlib import Path


def read(path):
    with open(path, encoding="utf-8", newline="") as f:
        r = csv.reader(f)
        next(r)
        return {tuple(row[:-1]): int(row[-1]) for row in r}


def main(folder="results"):
    p = Path(folder)
    summary = {}
    for k in (1, 2, 3):
        a = read(p / f"mapreduce_k{k}.csv")
        b = read(p / f"duckdb_k{k}.csv")
        differences = {
            x: (a.get(x), b.get(x)) for x in a.keys() | b.keys() if a.get(x) != b.get(x)
        }
        summary[f"k{k}"] = {
            "equal": not differences,
            "mapreduce_unique": len(a),
            "duckdb_unique": len(b),
            "mapreduce_total": sum(a.values()),
            "duckdb_total": sum(b.values()),
            "differences": len(differences),
            "examples": list(differences.items())[:5],
        }
    (p / "verification.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(summary, ensure_ascii=False))
    if any(not x["equal"] for x in summary.values()):
        sys.exit(1)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "results")
