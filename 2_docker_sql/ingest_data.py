#!/usr/bin/env python
# coding: utf-8

# # Load Dataset (NYC TLC Trip Record Data)
# https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# Data Dictionary (https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
import argparse
import os
from time import time

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def load_csv_data(filename, table_name, engine):
    # Read data in chunks of size 100_000 and create a dataframe iterator
    df_iter = pd.read_csv(filename, iterator=True, chunksize=100_000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    ### Create table yellow_taxi_data and if exists drop it and create new one
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

    ### Insert Data into table and append to it if exists
    df.to_sql(name=table_name, con=engine, if_exists="append")


    ### Inserting the rest of the data using loop
    try:
        while True:
            t_start = time()
            
            df = next(df_iter)
            
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists="append")
            
            t_end = time()
            
            print(f"Inserted another chunk of size {len(df)}: took {t_end - t_start:.3f} seconds")
    except StopIteration as e:
        print(str(e))
        print("Finished inserting data")


def load_parquet_data(parquet_file, table_name, engine):
    # Read data in chunks of size 100_000 and create a dataframe iterator
    pfile = pq.ParquetFile(parquet_file)
    table_iter = pfile.iter_batches(batch_size=100_000)
    table = next(table_iter)
    df = table.to_pandas()

    ### Create table yellow_taxi_data and if exists drop it and create new one
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

    ### Insert Data into table and append to it if exists
    df.to_sql(name=table_name, con=engine, if_exists="append")


    ### Inserting the rest of the data using loop
    try:
        while True:
            t_start = time()
        
            table = next(table_iter)
            df = table.to_pandas()
            df.to_sql(name=table_name, con=engine, if_exists="append")
            
            t_end = time()
            
            print(f"Inserted another chunk of size {len(df)}: took {t_end - t_start:.3f} seconds")
    except StopIteration as e:
        print(str(e))
        print("Finished inserting data")


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.db
    table_name = params.table_name
    url = params.url
    filename = url.split("/")[-1]
    
    
    # download the csv
    print(filename)
    print(os.path.exists(filename))
    if os.path.exists(filename):
        print(f"{filename} already downloaded")
    else:
        status = os.system(f"wget {url} -O {filename}")
        if status != 0:
            return "ERROR 404: File Not Found"
    
    # Connect to database
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    
    if filename.endswith("csv"):
        load_csv_data(filename, table_name, engine)
    
    if filename.endswith("parquet"):
        load_parquet_data(filename, table_name, engine)

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    # user, password, host, port, database name, table name
    # url of the csv
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port number for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name where we will write results to.')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    
    main(args)
