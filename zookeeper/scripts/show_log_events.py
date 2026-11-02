import argparse
import re
from pathlib import Path

EVENTS = re.compile(
    r"became_leader|restored_checkpoint|записал результат|BadVersionError"
)


def matching_lines(path):
    path = Path(path)
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if EVENTS.search(line):
            yield f"{path}:{number}:{line}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("logs", nargs="+", type=Path)
    args = parser.parse_args()
    for path in args.logs:
        for line in matching_lines(path):
            print(line)
