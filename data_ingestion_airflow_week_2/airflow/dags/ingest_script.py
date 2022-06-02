import os

from time import time

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def load_csv_data(filename, engine, table_name):
    t_start = time()
    df_iter = pd.read_csv(filename, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    t_end = time()
    print('inserted the first chunk, took %.3f second' % (t_end - t_start))

    while True: 
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("completed")
            break

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))


def load_parquet_data(filename, engine, table_name):
    t_start = time()
    # Read data in chunks of size 100_000 and create a dataframe iterator
    pfile = pq.ParquetFile(filename)
    table_iter = pfile.iter_batches(batch_size=100_000)
    table = next(table_iter)
    df = table.to_pandas()

    ### Create table yellow_taxi_data and if exists drop it and create new one
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

    ### Insert Data into table and append to it if exists
    df.to_sql(name=table_name, con=engine, if_exists="append")

    t_end = time()
    print('Inserted the first chunk, took %.3f second' % (t_end - t_start))

    while True: 
        t_start = time()

        try:
            table = next(table_iter)
        except StopIteration:
            print("completed")
            break

        df = table.to_pandas()
        df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()

        print('Inserted another chunk, took %.3f second' % (t_end - t_start))


def ingest_callable(user, password, host, port, db, table_name, filename):
    print(user, password, host, port, db, table_name, filename)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')
    
    if filename.endswith("csv"):
        load_csv_data(filename, engine, table_name)
    
    if filename.endswith("parquet"):
        load_parquet_data(filename, engine, table_name)



def ingest_callable_zones(user, password, host, port, db, table_name, filename):
    print(user, password, host, port, db, table_name, filename)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

    if filename.endswith("csv"):
        t_start = time()
        df = pd.read_csv(filename)

        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print('Inserted the zones data, took %.3f second' % (t_end - t_start))
    
    if filename.endswith("parquet"):
        t_start = time()
        
        df = pd.read_parquet(filename)
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print('Inserted the zones data, took %.3f second' % (t_end - t_start))
