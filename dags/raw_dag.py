"""DAG ingest_raw - CSV producteurs → raw/ avec partitionnement Hive."""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
from airflow.decorators import dag, task
from botocore.client import Config

# === Config ===
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
BUCKET = "raw"
DATA_DIR = Path("/opt/airflow/data")

FILES = [
    "LineA_Stable_10K.csv",
    "LineB_Flux.csv",
    "LineC_Turbulent.csv",
    "LineD_SpikeControl.csv",
    "LineE_SmoothRun.csv",
]

CHUNK_LINES = {"A"}   # lignes uploadées en chunks (simule flux)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def build_key(line_letter: str, year: int, month: int, filename: str) -> str:
    """Clé S3 Hive-partitionnée."""
    return (
        f"production_lines/"
        f"year={year}/month={month:02d}/line={line_letter.lower()}/"
        f"{filename}"
    )


def put_and_verify(s3, bucket: str, key: str, payload: bytes) -> dict:
    """Upload + vérif MD5 vs ETag."""
    expected = md5_bytes(payload)
    s3.put_object(Bucket=bucket, Key=key, Body=payload)
    etag = s3.head_object(Bucket=bucket, Key=key)["ETag"].strip('"')
    ok = expected == etag
    status = "✅" if ok else "❌"
    print(f"{status} {key}  ({len(payload)} bytes)")
    return {"key": key, "size": len(payload), "ok": ok}


@dag(
    dag_id="ingest_raw",
    description="Ingestion CSV producteurs → raw/ (Hive partitions)",
    start_date=datetime(2025, 1, 1),
    schedule=None,            # déclenchement manuel pour ce projet
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["ingestion", "raw"],
)
def ingest_raw():

    @task
    def ingest_file(filename: str) -> dict:
        local = DATA_DIR / filename
        line_letter = filename.split("_")[0].replace("Line", "")  # 'A'
        
        df = pd.read_csv(local, parse_dates=["timestamp"])
        df["_year"] = df["timestamp"].dt.year
        df["_month"] = df["timestamp"].dt.month
        s3 = get_s3_client()
        results = []

        if line_letter in CHUNK_LINES:
            # Chunking journalier pour simuler flux temps réel
            df["_date"] = df["timestamp"].dt.date
            for i, (date, grp) in enumerate(df.groupby("_date"), start=1):
                year, month = grp["_year"].iloc[0], grp["_month"].iloc[0]
                chunk_name = f"{Path(filename).stem}_chunk_{i:04d}.csv"
                key = build_key(line_letter, year, month, chunk_name)
                payload = grp.drop(columns=["_year", "_month", "_date"]).to_csv(
                    index=False
                ).encode("utf-8")
                results.append(put_and_verify(s3, BUCKET, key, payload))
        else:
            # Upload monolithique groupé par année/mois (1 seul dans notre cas)
            for (year, month), grp in df.groupby(["_year", "_month"]):
                key = build_key(line_letter, year, month, filename)
                payload = grp.drop(columns=["_year", "_month"]).to_csv(
                    index=False
                ).encode("utf-8")
                results.append(put_and_verify(s3, BUCKET, key, payload))

        return {"file": filename, "uploads": results,
                "all_ok": all(r["ok"] for r in results)}

    # Dynamic mapping : 5 task instances en parallèle
    ingest_file.expand(filename=FILES)


ingest_raw()