import os
from dotenv import load_dotenv
import boto3
import hashlib
from pathlib import Path
from botocore.client import Config


load_dotenv()
# === Config ===
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY =os.getenv("MINIO_ROOT_USER")  # ⚠ à charger depuis .env idéalement
SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")   # ⚠ à charger depuis .env idéalement
BUCKET = "raw"
LOCAL_DATA_DIR = Path("./raw/production_lines/")    # adapter au chemin réel

FILES = [
    "LineA_Stable_10K.csv",
    "LineB_Flux.csv",
    "LineC_Turbulent.csv",
    "LineD_SpikeControl.csv",
    "LineE_SmoothRun.csv",
]


def compute_md5(filepath: Path, chunk_size: int = 8192) -> str:
    """MD5 streamé (pas de chargement complet en mémoire)."""
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5.update(chunk)
    return md5.hexdigest()


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",   # placeholder, MinIO l'ignore
    )


def derive_line_id(filename: str) -> str:
    """LineA_Stable_10K.csv -> line_a"""
    prefix = filename.split("_")[0]   # 'LineA'
    letter = prefix.replace("Line", "").lower()  # 'a'
    return f"line_{letter}"


def upload_and_verify(s3, local_path: Path, bucket: str, key: str) -> bool:
    """Upload + vérif intégrité par comparaison MD5 / ETag."""
    local_md5 = compute_md5(local_path)
    
    # Upload (single-part auto pour fichiers < 5 GB)
    s3.upload_file(str(local_path), bucket, key)
    
    # Récupère ETag du fichier déposé
    head = s3.head_object(Bucket=bucket, Key=key)
    remote_etag = head["ETag"].strip('"')
    
    ok = (local_md5 == remote_etag)
    status = "✅" if ok else "❌"
    print(f"{status} {key}")
    print(f"   local  MD5  = {local_md5}")
    print(f"   remote ETag = {remote_etag}")
    return ok


def main():
    s3 = get_s3_client()
    results = []
    for filename in FILES:
        local = LOCAL_DATA_DIR / filename
        line_id = derive_line_id(filename)
        key = f"production_lines/{line_id}/{filename}"
        ok = upload_and_verify(s3, local, BUCKET, key)
        results.append((filename, ok))
    
    print("\n=== Bilan ===")
    for fname, ok in results:
        print(f"  {'OK' if ok else 'FAIL'}  {fname}")


if __name__ == "__main__":
    main()