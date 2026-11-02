import csv
import io
import os
import threading
import time

import boto3
import psutil
from botocore.config import Config

ROLE = "Аналитик"
BUCKET = os.getenv("S3_BUCKET", "hh-lake")
KEY = os.getenv("S3_KEY", "it_vacancies_2024_projection.csv")


def s3():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["RUSTFS_ACCESS_KEY"],
        aws_secret_access_key=os.environ["RUSTFS_SECRET_KEY"],
        region_name=os.getenv("S3_REGION", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 4}),
    )


def rows_from_s3():
    body = s3().get_object(Bucket=BUCKET, Key=KEY)["Body"]
    try:
        yield from csv.DictReader(io.TextIOWrapper(body, encoding="utf-8", newline=""))
    finally:
        body.close()


def skills(value):
    return tuple(sorted({x.strip().casefold() for x in value.split(";") if x.strip()}))


def salary(row):
    if row["currency"] != "RUR":
        return None
    a = row["salary_from"]
    b = row["salary_to"]
    if not (a or b):
        return None
    return (float(a) + float(b)) / 2 if a and b else float(a or b)


class Meter:

    def __enter__(self):
        self.proc = psutil.Process()
        self.peak = 0
        self.stop = False

        def sample():
            while not self.stop:
                self.peak = max(self.peak, self.proc.memory_info().rss)
                time.sleep(0.01)

        self.start = time.perf_counter()
        self.thread = threading.Thread(target=sample, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.elapsed = time.perf_counter() - self.start
        self.stop = True
        self.thread.join()
        self.peak = max(self.peak, self.proc.memory_info().rss)
