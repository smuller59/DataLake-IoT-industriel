import io
import os

import boto3, pandas as pd
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client("s3", endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
                  aws_access_key_id=os.environ["MINIO_ROOT_USER"],
                  aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
                  config=Config(signature_version="s3v4"))

key = "production_lines/year=2025/month=03/curated_multiline.parquet"
obj = s3.get_object(Bucket="curated", Key=key)
df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

print(df.columns.tolist())
print(df[["line_id", "temperature", "temp_zscore", "anomaly_score", "label"]].describe())
print(df.groupby("label")["anomaly_score"].describe())