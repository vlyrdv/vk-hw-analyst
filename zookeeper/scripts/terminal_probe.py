import argparse
import csv
import hashlib
import json
import logging
import socket
import sys
from pathlib import Path
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from core import connect

logging.basicConfig(level=logging.ERROR)


def run(args):
    if args.action == "write-read":
        z = connect()
        try:
            z.create(args.target, b"write with 2 of 3")
            value, stat = z.get(args.target)
            assert value == b"write with 2 of 3"
            print(
                json.dumps(
                    dict(
                        path=args.target,
                        value=value.decode(),
                        version=stat.version,
                        success=True,
                    ),
                    indent=2,
                )
            )
        finally:
            z.stop()
            z.close()
    elif args.action == "srvr":
        with socket.create_connection(
            ("127.0.0.1", int(args.target)), timeout=3
        ) as sock:
            sock.sendall(b"srvr")
            sock.shutdown(socket.SHUT_WR)
            parts = []
            while part := sock.recv(4096):
                parts.append(part)
        print(b"".join(parts).decode(), end="")
    elif args.action == "connect":
        z = KazooClient(hosts="127.0.0.1:" + args.target, timeout=4, read_only=False)
        try:
            z.start(timeout=5)
        except KazooTimeoutError:
            print(
                "KazooTimeoutError: writable session not established within 5 seconds"
            )
        else:
            raise AssertionError("Unexpected connection without quorum")
        finally:
            z.stop()
            z.close()
    elif args.action == "summary":
        source = json.loads((ROOT / args.target).read_text())
        result = {
            k: source[k]
            for k in [
                "run",
                "session_timeout_ms",
                "suspended_seconds",
                "failover_seconds",
                "checkpoint_next_row",
                "accepted_count",
                "pointer_version",
            ]
        }
        result["coordinator"] = source["pointer"]["coordinator"]
        result["epoch"] = source["pointer"]["epoch"]
        print(json.dumps(result, indent=2))
    elif args.action == "compare":
        reference = hashlib.sha256((ROOT / "data/hw3_k2.csv").read_bytes()).hexdigest()
        print("reference_sha256=" + reference)
        for run_name in [args.target, *args.extra]:
            files = sorted((ROOT / "results" / run_name).glob("*-pairs.csv"))
            assert len(files) == 2
            for file in files:
                digest = hashlib.sha256(file.read_bytes()).hexdigest()
                with file.open(newline="") as stream:
                    rows = list(csv.DictReader(stream))
                count, total = len(rows), sum(int(row["n"]) for row in rows)
                assert digest == reference and count == 96052 and total == 506354
                print(
                    json.dumps(
                        dict(
                            run=run_name,
                            file=file.name,
                            unique_pairs=count,
                            total_pairs=total,
                            equals_hw3=digest == reference,
                        )
                    )
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action", choices=["write-read", "srvr", "connect", "summary", "compare"]
    )
    parser.add_argument("target")
    parser.add_argument("extra", nargs="*")
    run(parser.parse_args())
