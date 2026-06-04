import io
import boto3, pandas as pd
from botocore.client import Config

s3 = boto3.client("s3", endpoint_url="http://localhost:9000",
                  aws_access_key_id="minioadmin",
                  aws_secret_access_key="minioadmin123",
                  config=Config(signature_version="s3v4"))

key = "production_lines/year=2025/month=04/line=b/LineB_Flux.parquet"
obj = s3.get_object(Bucket="staging", Key=key)

# ⬇ Lit tout en mémoire dans un buffer seekable
buf = io.BytesIO(obj["Body"].read())
df = pd.read_parquet(buf)

print(df.dtypes)
print(df.head(3))
print(df.columns.tolist())
print(df["line_id"].unique())