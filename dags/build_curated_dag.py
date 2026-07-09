"""DAG build_curated - staging/ (Parquet) → curated/ (features multi-lignes)."""
from __future__ import annotations

import io
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow.decorators import dag, task
from botocore.client import Config

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY = os.environ["MINIO_ROOT_USER"]
SECRET_KEY = os.environ["MINIO_ROOT_PASSWORD"]
STAGING_BUCKET = "staging"
CURATED_BUCKET = "curated"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


@dag(
    dag_id="build_curated",
    description="staging/ Parquet → curated/ features multi-lignes (z-score)",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["curated", "features"],
)
def build_curated():

    @task
    def list_staging_keys() -> list[str]:
        """Liste tous les Parquet dans staging/production_lines/."""
        s3 = get_s3_client()
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=STAGING_BUCKET, Prefix="production_lines/"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    keys.append(obj["Key"])
        if not keys:
            raise ValueError("Aucun Parquet dans staging/. Lance transform_to_staging d'abord.")
        print(f"Trouvé {len(keys)} fichier(s) Parquet à agréger")
        return sorted(keys)

    @task
    def build_curated_table(keys: list[str]) -> dict:
        """Charge tout staging/, calcule les features multi-lignes, écrit curated/."""
        s3 = get_s3_client()

        # 1. Charge et concatène TOUS les fichiers staging (petit volume, OK en mémoire)
        dfs = []
        for key in keys:
            obj = s3.get_object(Bucket=STAGING_BUCKET, Key=key)
            buf = io.BytesIO(obj["Body"].read())
            dfs.append(pd.read_parquet(buf))
        df = pd.concat(dfs, ignore_index=True)
        print(f"Dataset complet : {len(df)} lignes, {df['line_id'].nunique()} lignes de prod")

        # 2. Stats GLOBALES par ligne (pas par fichier/chunk)
        stats = df.groupby("line_id").agg(
            temp_mean=("temperature", "mean"),
            temp_std=("temperature", "std"),
            pressure_mean=("pressure", "mean"),
            pressure_std=("pressure", "std"),
        ).reset_index()
        print("Stats par ligne :")
        print(stats)

        # 3. Merge stats + calcul z-score (écart au comportement nominal DE SA PROPRE ligne)
        df = df.merge(stats, on="line_id", how="left")
        df["temp_zscore"] = (df["temperature"] - df["temp_mean"]) / df["temp_std"]
        df["pressure_zscore"] = (df["pressure"] - df["pressure_mean"]) / df["pressure_std"]

        # 4. Score d'anomalie composite (max des deux z-scores absolus)
        df["anomaly_score"] = df[["temp_zscore", "pressure_zscore"]].abs().max(axis=1)

        # 5. Nettoyage colonnes intermédiaires (garde stats de contexte, drop le bruit)
        df = df.drop(columns=["temp_mean", "temp_std", "pressure_mean", "pressure_std"])

        # 6. Partitionnement year/month (SANS line, cf. décision archi jour 1 : analyse cross-lignes)
        df["_year"] = df["timestamp"].dt.year
        df["_month"] = df["timestamp"].dt.month

        results = []
        for (year, month), grp in df.groupby(["_year", "_month"]):
            grp_out = grp.drop(columns=["_year", "_month"])
            key = f"production_lines/year={year}/month={month:02d}/curated_multiline.parquet"

            buf = io.BytesIO()
            grp_out.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
            payload = buf.getvalue()

            s3.put_object(Bucket=CURATED_BUCKET, Key=key, Body=payload)
            print(f"✅ {key}  rows={len(grp_out)}")
            results.append({"key": key, "rows": len(grp_out)})

        # 7. Résumé
        total_rows = len(df)
        anomaly_rate = (df["label"] == 1).mean() * 100
        print(f"\n=== Bilan curated ===")
        print(f"Lignes totales      : {total_rows}")
        print(f"Fichiers créés      : {len(results)}")
        print(f"Taux anomalie label : {anomaly_rate:.2f}%")
        print(f"Anomaly_score > 3   : {(df['anomaly_score'] > 3).sum()} enregistrements")

        return {
            "files_created": len(results),
            "total_rows": total_rows,
            "stats_per_line": stats.to_dict(orient="records"),
        }

    keys = list_staging_keys()
    build_curated_table(keys)


build_curated()