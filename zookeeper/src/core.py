import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from kazoo.client import KazooClient

HOSTS = os.getenv("ZK_HOSTS", "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183")
ROOT = Path(__file__).resolve().parents[1]


def encode(x):
    return json.dumps(x, ensure_ascii=False, sort_keys=True).encode()


def log(event, **fields):
    print(
        json.dumps(
            dict(
                time=datetime.now(timezone.utc).isoformat(),
                mono=time.monotonic(),
                event=event,
                **fields
            ),
            ensure_ascii=False,
        ),
        flush=True,
    )


def connect():
    z = KazooClient(hosts=HOSTS, timeout=10.0)
    z.start(timeout=20)
    return z


def commit(tx):
    values = tx.commit()
    for value in values:
        if isinstance(value, Exception):
            raise value
    return values
