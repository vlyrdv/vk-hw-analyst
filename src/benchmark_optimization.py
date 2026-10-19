import argparse, hashlib, json, os, subprocess, sys, tempfile
from pathlib import Path

RAW = "it_vacancies_2024.csv"
PROJECTED = "it_vacancies_2024_projection.csv"


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--output", default="results/optimization.json")
    args = parser.parse_args()
    if args.trials < 3:
        raise ValueError("Для отчёта нужно не менее трёх попыток")
    script = Path(__file__).with_name("mapreduce.py")
    runs = []
    temp_root = Path("work")
    temp_root.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="analyst_bonus_", dir=temp_root) as tmp:
        paths = {
            stage: Path(tmp) / stage for stage in ("before_raw", "after_projection")
        }
        for trial in range(1, args.trials + 1):
            for stage, key in [("before_raw", RAW), ("after_projection", PROJECTED)]:
                env = os.environ.copy()
                env["S3_KEY"] = key
                proc = subprocess.run(
                    [
                        sys.executable,
                        str(script),
                        "--outdir",
                        str(paths[stage]),
                        "--no-upload",
                    ],
                    env=env,
                    capture_output=True,
                    text=True,
                    check=True,
                )
                result = json.loads(proc.stdout.strip())
                result.update(trial=trial, stage=stage, object_key=key)
                runs.append(result)
                print(
                    stage, trial, result["seconds"], result["peak_rss_mb"], flush=True
                )
        hashes = {
            stage: [digest(paths[stage] / f"mapreduce_k{k}.csv") for k in (1, 2, 3)]
            for stage in paths
        }
        if hashes["before_raw"] != hashes["after_projection"]:
            raise AssertionError("Частоты до и после оптимизации различаются")
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(
            {
                "runs": runs,
                "sha256_equal": True,
                "sha256_k1_k2_k3": hashes["before_raw"],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print("sha256_equal", True)


if __name__ == "__main__":
    main()
