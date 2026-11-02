import hashlib
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from core import ROOT
from hw3_common import s3, BUCKET, KEY

client = s3()
for key, name in [
    (KEY, "it_vacancies_2024_projection.csv"),
    ("results/analyst/mapreduce_k2.csv", "hw3_k2.csv"),
]:
    dest = ROOT / "data" / name
    dest.parent.mkdir(exist_ok=True)
    client.download_file(BUCKET, key, str(dest))
    print(
        json.dumps(
            dict(
                bucket=BUCKET,
                key=key,
                file=name,
                bytes=dest.stat().st_size,
                sha256=hashlib.sha256(dest.read_bytes()).hexdigest(),
            )
        )
    )
