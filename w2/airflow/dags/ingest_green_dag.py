import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import logging
from data_ingestion import download, ingest_data, DBConf

logger = logging.getLogger("data_ingestion")
logger.setLevel(logging.INFO)

chain_conf = lambda key, default=None: os.environ.get(
    key, Variable.get(key, default_var=default)
)

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME", "/opt/airflow")

PG_HOST = chain_conf("PG_HOST")
PG_USER = chain_conf("PG_USER")
PG_PASSWORD = chain_conf("PG_PASSWORD")
PG_PORT = chain_conf("PG_PORT")
PG_DATABASE = chain_conf("PG_DATABASE")

YEAR_MONTH = "{{logical_date.strftime('%Y-%m')}}"
DATASET_URL = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{YEAR_MONTH}.parquet"
OUTPUT_FILE = f"{AIRFLOW_HOME}/green_tripdata_{YEAR_MONTH}.parquet"
TABLE_NAME = f"green_taxi_{YEAR_MONTH}"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

local_workflow = DAG(
    "local_ingesting_green_dag",
    default_args=default_args,
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
)

with local_workflow:
    download_task = PythonOperator(
        task_id="download_task",
        python_callable=download,
        op_kwargs={"url": DATASET_URL, "dest": OUTPUT_FILE},
    )

    ingest_task = PythonOperator(
        task_id="ingest_task",
        python_callable=ingest_data,
        op_kwargs={
            "file": OUTPUT_FILE,
            "db_conf": DBConf(
                host=PG_HOST,
                user=PG_USER,
                password=PG_PASSWORD,
                port=PG_PORT,
                database=PG_DATABASE,
            ),
            "table": TABLE_NAME,
        },
    )
    cleanup_task = BashOperator(
        task_id="cleanup_task",
        bash_command="test -f $file && rm $file",
        env={"file": OUTPUT_FILE},
    )
    download_task >> ingest_task >> cleanup_task
