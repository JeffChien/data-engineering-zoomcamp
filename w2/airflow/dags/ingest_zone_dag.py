import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import logging
from data_ingestion import download, ingest_data, DBConf, csv_to_parquet
import textwrap

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

DATASET_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
CSV_OUTPUT_FILE = f"{AIRFLOW_HOME}/taxi_zone_lookup.csv"
PARQUET_OUTPUT_FILE = f"{AIRFLOW_HOME}/taxi_zone_lookup.parquet"
TABLE_NAME = "taxi_zone_lookup"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

local_workflow = DAG(
    "local_ingesting_zone_dag",
    default_args=default_args,
    schedule_interval="@once",
    start_date=datetime(2019, 1, 1),
)

with local_workflow:
    download_task = PythonOperator(
        task_id="download_task",
        python_callable=download,
        op_kwargs={"url": DATASET_URL, "dest": CSV_OUTPUT_FILE},
    )

    format_parquet_task = PythonOperator(
        task_id="format_parquet_task",
        python_callable=csv_to_parquet,
        op_kwargs={"csv_file": CSV_OUTPUT_FILE, "output_file": PARQUET_OUTPUT_FILE},
    )

    ingest_task = PythonOperator(
        task_id="ingest_task",
        python_callable=ingest_data,
        op_kwargs={
            "file": PARQUET_OUTPUT_FILE,
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
        bash_command=textwrap.dedent(
            """
            test -f $file1 && rm $file1
            test -f $file2 && rm $file2
        """
        ),
        env={"file1": CSV_OUTPUT_FILE, "file2": PARQUET_OUTPUT_FILE},
    )
    download_task >> format_parquet_task >> ingest_task >> cleanup_task
