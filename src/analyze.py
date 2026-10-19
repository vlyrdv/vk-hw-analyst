import json
import statistics
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from common import ROLE, rows_from_s3, salary, skills

REGIONS = ("Москва", "Санкт-Петербург", "Екатеринбург")


def median(v):
    return round(statistics.median(v)) if v else None


def top(counter, n=5):
    return [
        {"skills": list(k) if isinstance(k, tuple) else [k], "n": v}
        for k, v in counter.most_common(n)
    ]


def main(outdir="results"):
    out = Path(outdir)
    out.mkdir(parents=True, exist_ok=True)
    counts = {k: Counter() for k in (1, 2, 3)}
    paid = {k: defaultdict(list) for k in (1, 2, 3)}
    regions = {
        r: {
            "vacancies": 0,
            "salary": [],
            "skills": Counter(),
            "pairs": Counter(),
            "triples": Counter(),
        }
        for r in REGIONS
    }
    total = 0
    with_skills = 0
    paid_n = 0
    for row in rows_from_s3():
        if row["role"] != ROLE:
            continue
        total += 1
        ss = skills(row["key_skills"])
        with_skills += bool(ss)
        sal = salary(row)
        paid_n += sal is not None
        reg = regions.get(row["area"])
        if reg:
            reg["vacancies"] += 1
            if sal is not None:
                reg["salary"].append(sal)
        for k in (1, 2, 3):
            for combo in combinations(ss, k):
                counts[k][combo] += 1
                if sal is not None:
                    paid[k][combo].append(sal)
                if reg:
                    reg[{1: "skills", 2: "pairs", 3: "triples"}[k]][combo] += 1
    lifts = {}
    for k in (2, 3):
        arr = []
        for combo, values in paid[k].items():
            if len(values) < 30:
                continue
            parents = list(combinations(combo, k - 1))
            base = max(parents, key=lambda x: (counts[k - 1][x], x))
            basevals = paid[k - 1][base]
            if len(basevals) < 30:
                continue
            m = median(values)
            bm = median(basevals)
            arr.append(
                {
                    "skills": list(combo),
                    "n_salary": len(values),
                    "median_rub": m,
                    "baseline_skills": list(base),
                    "baseline_n_salary": len(basevals),
                    "baseline_median_rub": bm,
                    "delta_rub": m - bm,
                    "delta_pct": round((m / bm - 1) * 100, 1) if bm else None,
                }
            )
        lifts[k] = {
            "up": sorted(arr, key=lambda x: (-x["delta_rub"], -x["n_salary"]))[:10],
            "down": sorted(arr, key=lambda x: (x["delta_rub"], -x["n_salary"]))[:10],
            "eligible_combinations": len(arr),
        }
    region_summary = {
        r: {
            "vacancies": v["vacancies"],
            "salary_n": len(v["salary"]),
            "median_salary_rub": median(v["salary"]),
            "top_skills": top(v["skills"]),
            "top_pairs": top(v["pairs"]),
            "top_triples": top(v["triples"]),
        }
        for r, v in regions.items()
    }
    result = {
        "total_analyst": total,
        "with_skills": with_skills,
        "salary_rur_n": paid_n,
        "regions": region_summary,
        "lifts": lifts,
        "top_global": {k: top(counts[k], 10) for k in (1, 2, 3)},
    }
    (out / "analysis.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "total": total,
                "salary_rur_n": paid_n,
                "regions": {k: v["vacancies"] for k, v in region_summary.items()},
                "eligible_pairs": lifts[2]["eligible_combinations"],
                "eligible_triples": lifts[3]["eligible_combinations"],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    import sys

    main(sys.argv[1] if len(sys.argv) > 1 else "results")
