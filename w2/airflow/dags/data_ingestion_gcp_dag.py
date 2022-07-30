import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)
import pyarrow.csv as pv
import pyarrow.parquet as pq
import logging
from urllib import request

logger = logging.getLogger("data_ingestion")
logger.setLevel(logging.INFO)

chain_conf = lambda key, default=None: os.environ.get(key, Variable.get(key, default_var=default))

PROJECT_ID = chain_conf("GCP_PROJECT_ID")
BUCKET = chain_conf("GCP_GCS_BUCKET")

dataset_file = "yellow_tripdata_2021-01.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_local_home = chain_conf("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = chain_conf("BIGQUERY_DATASET", "trips_data_all")


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def download(url, dest):
    try:
        logger.info(f"Downloading from {url}")
        res = request.urlopen(url)
        with open(dest, "wb+") as f:
            f.write(res.read())
            logger.info(f"save to {dest}")
    except Exception as err:
        logger.error(err)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["dtc-de"],
) as dag:

    download_task = PythonOperator(
        task_id="download_task",
        python_callable=download,
        op_kwargs={"url": dataset_url, "dest": dataset_file},
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{dataset_file}",
            "local_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{dataset_file}"],
            },
        },
    )

    (
        download_task
        >> local_to_gcs_task
        >> bigquery_external_table_task
    )
