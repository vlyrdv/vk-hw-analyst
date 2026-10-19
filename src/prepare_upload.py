import argparse, csv, os
from pathlib import Path
from common import s3

FIELDS = [
    "vacancy_id",
    "role",
    "area",
    "salary_from",
    "salary_to",
    "currency",
    "key_skills",
]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("source")
    p.add_argument("--projection", default="work/it_vacancies_2024_projection.csv")
    p.add_argument("--variant-file", default="work/analyst_variant10.csv")
    a = p.parse_args()
    s = s3()
    bucket = os.getenv("S3_BUCKET", "hh-lake")
    try:
        s.head_bucket(Bucket=bucket)
    except Exception:
        s.create_bucket(Bucket=bucket)
    path = Path(a.projection)
    variant = Path(a.variant_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    variant.parent.mkdir(parents=True, exist_ok=True)
    n = nv = 0
    with open(a.source, encoding="utf-8-sig", newline="") as src, open(
        path, "w", encoding="utf-8", newline=""
    ) as dst, open(variant, "w", encoding="utf-8", newline="") as vd:
        r = csv.DictReader(src)
        w = csv.DictWriter(dst, fieldnames=FIELDS)
        vw = csv.DictWriter(vd, fieldnames=FIELDS)
        w.writeheader()
        vw.writeheader()
        for row in r:
            projected = {k: row[k] for k in FIELDS}
            w.writerow(projected)
            n += 1
            if row["role"] == "Аналитик":
                vw.writerow(projected)
                nv += 1
    s.upload_file(a.source, bucket, Path(a.source).name)
    s.upload_file(str(path), bucket, path.name)
    s.upload_file(str(variant), bucket, variant.name)
    for key in (Path(a.source).name, path.name, variant.name):
        h = s.head_object(Bucket=bucket, Key=key)
        print(f's3://{bucket}/{key} bytes={h["ContentLength"]}')
    print("rows", n, "analyst_rows", nv)


if __name__ == "__main__":
    main()
