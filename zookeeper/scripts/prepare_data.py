import gzip
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parents[1]
manifest = json.loads((root / "results/source-manifest.json").read_text())
for entry, name in zip(
    manifest["files"], ["it_vacancies_2024_projection.csv", "hw3_k2.csv"]
):
    path = root / "data" / name
    path.write_bytes(
        gzip.decompress(path.with_suffix(path.suffix + ".gz").read_bytes())
    )
    assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["sha256"]
    print(f"{name}: {path.stat().st_size} bytes, SHA-256 OK")
