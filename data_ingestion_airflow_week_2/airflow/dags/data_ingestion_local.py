import os

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1),
    tags=['dtc-de-local'],
)


URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data' 
URL_TEMPLATE = f'{URL_PREFIX}/yellow_tripdata_{datetime(2021, 1, 1).strftime("%Y-%m")}.parquet'
OUTPUT_FILE_TEMPLATE = f'{AIRFLOW_HOME}/output_{datetime(2021, 1, 1).strftime("%Y-%m")}.parquet'
TABLE_NAME_TEMPLATE = f'yellow_taxi_{datetime(2021, 1, 1).strftime("%Y_%m")}'

with local_workflow:
    wget_task = BashOperator(
        task_id='down_load_file',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = PythonOperator(
        task_id="ingest_csv_data_in_local_postgres_db",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            filename=OUTPUT_FILE_TEMPLATE
        ),
    )

    wget_task >> ingest_task