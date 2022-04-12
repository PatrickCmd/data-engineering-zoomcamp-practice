#!/usr/bin/env python
# coding: utf-8

# # Load Dataset (NYC TLC Trip Record Data)
# https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page
# Data Dictionary (https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
import argparse
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "output.csv"
    
    # download the csv
    if os.path.exists(csv_name):
        print(f"{csv_name} already downloaded")
    else:
        os.system(f"wget {url} -O {csv_name}")
    
    # Connect to database
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

    # Read data in chunks of size 100_000 and create a dataframe iterator
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100_000)

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
    except Exception as e:
        print(str(e))
        print("Finished inserting data")
        
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
