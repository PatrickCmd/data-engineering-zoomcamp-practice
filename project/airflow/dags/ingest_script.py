import os
from time import time

import pandas as pd
import zipfile
from sqlalchemy import create_engine


def load_csv_data(filename, engine, table_name):
    t_start = time()
    with zipfile.ZipFile(filename, "r") as f:
        filename = f.namelist()[0]
        with f.open(filename) as csv_file:
            df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100_000)
    
            df = next(df_iter)
            
            df.started_at = pd.to_datetime(df.started_at)
            df.ended_at = pd.to_datetime(df.ended_at)
            
            df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()
            print('inserted the first chunk in %s, took %.3f second' % (table_name, t_end - t_start))

            while True: 
                t_start = time()

                try:
                    df = next(df_iter)
                except StopIteration:
                    print("completed")
                    break

                df.started_at = pd.to_datetime(df.started_at)
                df.ended_at = pd.to_datetime(df.ended_at)

                df.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()

                print('inserted another chunk in %s, took %.3f second' % (tablename, t_end - t_start))


def ingest_callable(user, password, host, port, db, table_name, filename):
    print(user, password, host, port, db, table_name, filename)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')
    
    load_csv_data(filename, engine, table_name)
