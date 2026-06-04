"""DAG transform_to_staging - raw/ (CSV) → staging/ (Parquet harmonisé)."""
from __future__ import annotations

import io
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow.decorators import dag, task
from botocore.client import Config

# === Config ===
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
RAW_BUCKET = "raw"
STAGING_BUCKET = "staging"

# Règle d'harmonisation (cf. étape 2 — NOMS_CIBLE)
COLUMN_MAP = {
    "Temperature":  "temperature",
    "temperature":  "temperature",
    "Pressure":     "pressure",
    "pressure":     "pressure",
    "Elapsed_time": "elapsed_time",
    "elapsed_time": "elapsed_time",
    "timestamp":    "timestamp",
    "label":        "label",
}

REQUIRED_COLS = {"timestamp", "temperature", "pressure", "label",
                 "line_id", "ingestion_ts"}


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def extract_line_id(raw_key: str) -> str:
    """production_lines/year=2025/month=05/line=a/... → line_a"""
    for part in raw_key.split("/"):
        if part.startswith("line="):
            return f"line_{part.split('=', 1)[1]}"
    raise ValueError(f"Pas de partition 'line=' dans {raw_key}")


def validate_schema(df: pd.DataFrame, source: str) -> None:
    """Fail fast si schéma non conforme."""
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"[{source}] Colonnes manquantes: {missing}")
    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        raise TypeError(f"[{source}] timestamp non datetime: {df['timestamp'].dtype}")
    if not pd.api.types.is_integer_dtype(df["label"]):
        raise TypeError(f"[{source}] label non int: {df['label'].dtype}")


@dag(
    dag_id="transform_to_staging",
    description="raw/ CSV → staging/ Parquet harmonisé",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["transformation", "staging"],
)
def transform_to_staging():

    @task
    def list_raw_csvs() -> list[str]:
        """Liste toutes les clés CSV dans raw/production_lines/."""
        s3 = get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=RAW_BUCKET, Prefix="production_lines/"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".csv"):
                    keys.append(obj["Key"])
        if not keys:
            raise ValueError("Aucun CSV dans raw/. Lance ingest_raw d'abord.")
        print(f"Trouvé {len(keys)} fichier(s) à transformer")
        return sorted(keys)

    @task
    def transform_one(raw_key: str) -> dict:
        """Lit, transforme, écrit en Parquet vers staging/."""
        s3 = get_s3_client()

        # 1. Lecture depuis raw/
        obj = s3.get_object(Bucket=RAW_BUCKET, Key=raw_key)
        df = pd.read_csv(obj["Body"])

        # 2. Harmonisation noms (snake_case via COLUMN_MAP)
        rename_map = {c: COLUMN_MAP[c] for c in df.columns if c in COLUMN_MAP}
        unknown = set(df.columns) - set(COLUMN_MAP)
        if unknown:
            raise ValueError(f"Colonnes inconnues dans {raw_key}: {unknown}")
        df = df.rename(columns=rename_map)

        # 3. Typage timestamp str → datetime
        df["timestamp"] = pd.to_datetime(df["timestamp"])

        # 4. Ajout métadonnées staging
        df["line_id"] = extract_line_id(raw_key)
        df["ingestion_ts"] = pd.Timestamp.now(tz="UTC")

        # 5. Validation schéma (fail fast)
        validate_schema(df, source=raw_key)

        # 6. Sérialisation Parquet
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
        payload = buf.getvalue()

        # 7. Clé staging (même partition, extension .parquet)
        staging_key = raw_key.replace(".csv", ".parquet")

        # 8. Upload
        s3.put_object(Bucket=STAGING_BUCKET, Key=staging_key, Body=payload)

        ratio = round(len(payload) / obj["ContentLength"], 2)
        print(f"✅ {staging_key}  rows={len(df)}  ratio_parquet/csv={ratio}")
        return {
            "raw_key": raw_key,
            "staging_key": staging_key,
            "rows": len(df),
            "bytes_csv": obj["ContentLength"],
            "bytes_parquet": len(payload),
        }

    @task
    def summary(results: list[dict]) -> dict:
        """Bilan global pour audit."""
        total_rows = sum(r["rows"] for r in results)
        total_csv = sum(r["bytes_csv"] for r in results)
        total_pq = sum(r["bytes_parquet"] for r in results)
        ratio = round(total_pq / total_csv, 2) if total_csv else 0
        print(f"\n=== Bilan ===")
        print(f"Fichiers traités : {len(results)}")
        print(f"Lignes totales   : {total_rows:,}")
        print(f"Taille CSV       : {total_csv:,} B")
        print(f"Taille Parquet   : {total_pq:,} B")
        print(f"Ratio compression: {ratio} (×{round(1/ratio, 1)} gain)")
        return {"files": len(results), "rows": total_rows, "ratio": ratio}

    keys = list_raw_csvs()
    results = transform_one.expand(raw_key=keys)
    summary(results)


transform_to_staging()