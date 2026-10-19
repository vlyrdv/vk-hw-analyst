import json, os
from collections import Counter
from pathlib import Path

os.environ["S3_KEY"] = "it_vacancies_2024.csv"
from common import ROLE, rows_from_s3, skills


def main(output="results/drop_diagnostic.json"):
    groups = {
        "bpmn": {
            "areas": Counter(),
            "experience": Counter(),
            "titles": Counter(),
            "n": 0,
        },
        "bpmn_crm": {
            "areas": Counter(),
            "experience": Counter(),
            "titles": Counter(),
            "n": 0,
        },
    }
    for row in rows_from_s3():
        if (
            row["role"] != ROLE
            or row["currency"] != "RUR"
            or not (row["salary_from"] or row["salary_to"])
        ):
            continue
        ss = set(skills(row["key_skills"]))
        if "bpmn" not in ss:
            continue
        for key in (["bpmn", "bpmn_crm"] if "crm" in ss else ["bpmn"]):
            g = groups[key]
            g["n"] += 1
            g["areas"][row["area"]] += 1
            g["experience"][row["experience"]] += 1
            g["titles"][row["name"]] += 1
    result = {
        key: {
            "n": v["n"],
            "moscow_n": v["areas"]["Москва"],
            "experience_3to6_n": v["experience"]["От 3 до 6 лет"],
            "top_titles": v["titles"].most_common(5),
        }
        for key, v in groups.items()
    }
    p = Path(output)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    import sys

    main(sys.argv[1] if len(sys.argv) > 1 else "results/drop_diagnostic.json")
