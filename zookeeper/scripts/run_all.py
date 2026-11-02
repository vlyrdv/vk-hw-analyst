import datetime
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
tag = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S")
with (root / "logs" / f"ensemble-{tag}.txt").open("w") as f:
    subprocess.run(
        [sys.executable, str(root / "scripts/ensemble.py")],
        stdout=f,
        stderr=subprocess.STDOUT,
        check=True,
    )
for fencing in (0, 1):
    run = f"repeat-{tag}-fencing-{fencing}"
    with (root / "logs" / f"{run}-controller.log").open("w") as f:
        subprocess.run(
            [
                sys.executable,
                str(root / "scripts/failover.py"),
                "--fencing",
                str(fencing),
                "--run",
                run,
            ],
            stdout=f,
            stderr=subprocess.STDOUT,
            check=True,
        )
    print(root / "results" / run / "summary.json", flush=True)
