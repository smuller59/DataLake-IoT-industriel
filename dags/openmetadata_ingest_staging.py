"""DAG catalog_ingest_staging - scanne staging/ (MinIO) et alimente OpenMetadata."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def metadata_ingestion_workflow():
    import os

    import yaml
    from airflow.models import Variable
    from metadata.workflow.metadata import MetadataWorkflow

    jwt_token = os.environ["OPENMETADATA_JWT_TOKEN"]
    minio_access_key = os.environ["MINIO_ROOT_USER"]
    minio_secret_key = os.environ["MINIO_ROOT_PASSWORD"]

    config = f"""
source:
  type: datalake
  serviceName: minio-datalake-staging
  serviceConnection:
    config:
      type: Datalake
      configSource:
        securityConfig:
          awsAccessKeyId: {minio_access_key}
          awsSecretAccessKey: {minio_secret_key}
          awsRegion: us-east-1
          endPointURL: http://minio:9000
      bucketName: staging
      prefix: production_lines/
  sourceConfig:
    config:
      type: DatabaseMetadata
      markDeletedTables: true
      includeTables: true

sink:
  type: metadata-rest
  config: {{}}

workflowConfig:
  openMetadataServerConfig:
    hostPort: http://openmeta-server:8585/api
    authProvider: openmetadata
    securityConfig:
      jwtToken: {jwt_token}
"""
    workflow_config = yaml.safe_load(config)
    workflow = MetadataWorkflow.create(workflow_config)
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()


default_args = {
    "owner": "data-engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="catalog_ingest_staging",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["catalogue", "openmetadata"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_staging_metadata",
        python_callable=metadata_ingestion_workflow,
    )