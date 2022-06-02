import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable, ingest_callable_zones


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


default_args = {
    "owner": "airflow",
    #"start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


def download_load_dag(
    dag,
    url_template,
    local_filename_path_template,
    table_name_template,
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {local_filename_path_template}"
        )
        
        python_callable = ingest_callable if 'zone' not in table_name_template else ingest_callable_zones
        ingest_task = PythonOperator(
            task_id="ingest_csv_data_in_local_postgres_db",
            python_callable=python_callable,
            op_kwargs=dict(
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
                port=PG_PORT,
                db=PG_DATABASE,
                table_name=table_name_template,
                filename=local_filename_path_template
            ),
        )
        
        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_filename_path_template}"
        )
        
        download_dataset_task >> ingest_task >> rm_task


URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'

YELLOW_TAXI_URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ ds[:-3] }}.parquet'
YELLOW_TAXI_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_tripdata_{{ ds[:-3] }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ ds[:-3] }}'


yellow_taxi_data_dag = DAG(
    dag_id="yellow_taxi_data_local_homework_ds",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de-local-homework'],
)

download_load_dag(
    dag=yellow_taxi_data_dag,
    url_template=YELLOW_TAXI_URL_TEMPLATE,
    local_filename_path_template=YELLOW_TAXI_FILE_TEMPLATE,
    table_name_template=TABLE_NAME_TEMPLATE,
)


# https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2021-01.csv

GREEN_TAXI_URL_TEMPLATE = URL_PREFIX + '/green_tripdata_{{ ds[:-3] }}.parquet'
GREEN_TAXI_FILE_TEMPLATE = AIRFLOW_HOME + '/green_tripdata_{{ ds[:-3] }}.parquet'
GREEN_TABLE_NAME_TEMPLATE = 'green_tripdata_{{ ds[:-3].replace(\'-\', \'_\') }}'

green_taxi_data_dag = DAG(
    dag_id="green_taxi_data_local_homework",
    schedule_interval="0 7 2 * *",
    start_date=datetime(2019, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de-local-homework'],
)

download_load_dag(
    dag=green_taxi_data_dag,
    url_template=GREEN_TAXI_URL_TEMPLATE,
    local_filename_path_template=GREEN_TAXI_FILE_TEMPLATE,
    table_name_template=GREEN_TABLE_NAME_TEMPLATE,
)


# https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2021-01.csv

FHV_TAXI_URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ ds[:-3] }}.parquet'
FHV_TAXI_FILE_TEMPLATE = AIRFLOW_HOME + '/fhv_tripdata_{{ ds[:-3] }}.parquet'
FHV_TABLE_NAME_TEMPLATE = 'fhv_tripdata_{{ ds[:-3].replace(\'-\', \'_\') }}'

fhv_taxi_data_dag = DAG(
    dag_id="fhv_taxi_data_local_homework",
    schedule_interval="0 8 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
    tags=['dtc-de-local-homework'],
)

download_load_dag(
    dag=fhv_taxi_data_dag,
    url_template=FHV_TAXI_URL_TEMPLATE,
    local_filename_path_template=FHV_TAXI_FILE_TEMPLATE,
    table_name_template=FHV_TABLE_NAME_TEMPLATE,
)

# https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

ZONES_URL_TEMPLATE = 'https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv'
ZONES_FILE_TEMPLATE = AIRFLOW_HOME + '/taxi_zone_lookup.csv'
ZONES_TABLE_NAME_TEMPLATE="taxi_zone_lookup"

zones_data_dag = DAG(
    dag_id="zones_data_local_homework",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['dtc-de-local-homework'],
)

download_load_dag(
    dag=zones_data_dag,
    url_template=ZONES_URL_TEMPLATE,
    local_filename_path_template=ZONES_FILE_TEMPLATE,
    table_name_template=ZONES_TABLE_NAME_TEMPLATE,
)
