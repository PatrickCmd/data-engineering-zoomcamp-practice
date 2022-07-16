import os
import logging

from datetime import datetime
import zipfile
import pandas as pd

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

def print_dataframe(file):
    with zipfile.ZipFile(file, "r") as f:
        print(f"files: {f.namelist()}")
        filename = f.namelist()[0]
        with f.open(filename) as csv_file:
            df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100)
    df = next(df_iter)
    print(df.columns)


local_workflow = DAG(
    dag_id="Citi_Bikes_Local_Ingestion",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 5, 1),
    end_date=datetime(2022, 6, 1),
    catchup=True,
    max_active_runs=3,
    tags=["citi-bikes-local"],
)

# https://s3.amazonaws.com/tripdata/JC-202205-citibike-tripdata.csv.zip
URL_PREFIX = 'https://s3.amazonaws.com/tripdata' 
URL_TEMPLATE = URL_PREFIX + '/JC-{{ ds_nodash[:-2] }}-citibike-tripdata.csv.zip'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/JC-{{ ds_nodash[:-2] }}-citibike-tripdata.csv.zip'
TABLE_NAME_TEMPLATE = 'JC-{{ ds_nodash[:-2] }}-citibike-tripdata'

with local_workflow:
    download_datasets_task = BashOperator(
        task_id="download_files",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )
    
    print_dataframe = PythonOperator(
        task_id="print_dataframe",
        python_callable=print_dataframe,
        op_kwargs=dict(file=OUTPUT_FILE_TEMPLATE),
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
    
    # Delete downloaded files from container
    rm_task = BashOperator(
        task_id="rm_task",
        bash_command=f"rm {OUTPUT_FILE_TEMPLATE}"
    )
    
    # workflow dags
    # download_datasets_task >> rm_task
    download_datasets_task >> print_dataframe >> ingest_task >> rm_task
