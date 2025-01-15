#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_names = params.table_names
    urls = params.urls

    print(table_names, urls)

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    for i in range(len(urls)):
        if urls[i].endswith('.csv.gz'):
            csv_name = f'output{i}.csv.gz'
        else:
            csv_name = f'output{i}.csv'

        os.system(f"wget {urls[i]} -O {csv_name}")

        df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
        df = next(df_iter)
        if 'tpep_pickup_datetime' in df.columns:
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.head(n=0).to_sql(
            name=table_names[i], con=engine, if_exists='replace')

        df.to_sql(name=table_names[i], con=engine, if_exists='append')

        while True:

            try:
                t_start = time()

                df = next(df_iter)
                if 'tpep_pickup_datetime' in df.columns:
                    df.tpep_pickup_datetime = pd.to_datetime(
                        df.tpep_pickup_datetime)
                    df.tpep_dropoff_datetime = pd.to_datetime(
                        df.tpep_dropoff_datetime)

                df.to_sql(name=table_names[i], con=engine, if_exists='append')

                t_end = time()

                print('inserted another chunk, took %.3f second' %
                      (t_end - t_start))

            except StopIteration:
                print("Finished ingesting data into the postgres database")
                break


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True,
                        help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True,
                        help='database name for postgres')
    parser.add_argument('--table_names', nargs='+', required=True,
                        help='list of name of the tables where we will write the results to')
    parser.add_argument('--urls', nargs='+', required=True,
                        help='list of urls of the csv file')

    args = parser.parse_args()

    main(args)
