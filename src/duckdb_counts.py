import argparse
import csv
import json
import os
from pathlib import Path
from urllib.parse import urlparse
import duckdb
from common import Meter, BUCKET, KEY, s3


def sq(s):
    return "'" + s.replace("'", "''") + "'"


def run(outdir, upload=True):
    u = urlparse(os.environ["S3_ENDPOINT"])
    host = u.netloc
    db = duckdb.connect()
    db.execute("INSTALL httpfs")
    db.execute("LOAD httpfs")
    db.execute(
        f"CREATE SECRET s3_local (TYPE s3, KEY_ID {sq(os.environ['RUSTFS_ACCESS_KEY'])}, SECRET {sq(os.environ['RUSTFS_SECRET_KEY'])}, REGION {sq(os.getenv('S3_REGION','us-east-1'))}, ENDPOINT {sq(host)}, URL_STYLE 'path', USE_SSL {str(u.scheme=='https').lower()})"
    )
    uri = f"s3://{BUCKET}/{KEY}"
    with Meter() as meter:
        db.execute(f"""CREATE TEMP TABLE analyst AS
            SELECT vacancy_id, key_skills
            FROM read_csv({sq(uri)}, header=true, all_varchar=true)
            WHERE role = 'Аналитик'""")
        db.execute(
            """CREATE TEMP TABLE skill_rows AS
          WITH tokens AS (
            SELECT vacancy_id, trim(lower(unnest(string_split(key_skills, ';')))) AS skill FROM analyst
          ), distinct_tokens AS (
            SELECT DISTINCT vacancy_id, skill FROM tokens WHERE skill <> ''
          ) SELECT vacancy_id, skill, row_number() OVER (PARTITION BY vacancy_id ORDER BY skill) AS pos FROM distinct_tokens"""
        )
        queries = {
            1: """SELECT skill AS skill1, count(*) AS n
                  FROM skill_rows GROUP BY 1 ORDER BY 1""",
            2: """SELECT a.skill AS skill1, b.skill AS skill2, count(*) AS n
                  FROM skill_rows a
                  JOIN skill_rows b ON a.vacancy_id = b.vacancy_id AND a.pos < b.pos
                  GROUP BY 1, 2 ORDER BY 1, 2""",
            3: """SELECT a.skill AS skill1, b.skill AS skill2,
                         c.skill AS skill3, count(*) AS n
                  FROM skill_rows a
                  JOIN skill_rows b ON a.vacancy_id = b.vacancy_id AND a.pos < b.pos
                  JOIN skill_rows c ON a.vacancy_id = c.vacancy_id AND b.pos < c.pos
                  GROUP BY 1, 2, 3 ORDER BY 1, 2, 3""",
        }
        outputs = {k: db.execute(q).fetchall() for k, q in queries.items()}
        matched = db.execute("SELECT count(*) FROM analyst").fetchone()[0]
        has_skills = db.execute(
            "SELECT count(DISTINCT vacancy_id) FROM skill_rows"
        ).fetchone()[0]
    Path(outdir).mkdir(parents=True, exist_ok=True)
    client = s3() if upload else None
    for k, rows in outputs.items():
        path = Path(outdir) / f"duckdb_k{k}.csv"
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow([f"skill{i}" for i in range(1, k + 1)] + ["n"])
            w.writerows(rows)
        if client:
            client.upload_file(str(path), BUCKET, f"results/analyst/duckdb_k{k}.csv")
    return {
        "method": "duckdb",
        "seconds": round(meter.elapsed, 3),
        "peak_rss_mb": round(meter.peak / 1048576, 2),
        "matched_vacancies": matched,
        "vacancies_with_skills": has_skills,
        **{f"k{k}_unique": len(rows) for k, rows in outputs.items()},
        **{f"k{k}_total": sum(r[-1] for r in rows) for k, rows in outputs.items()},
    }


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--outdir", default="results")
    p.add_argument("--no-upload", action="store_true")
    a = p.parse_args()
    print(json.dumps(run(a.outdir, not a.no_upload), ensure_ascii=False))
