#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from time import time
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import pyarrow as pa


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = int(params.port)
    db = params.db
    table_name = params.table_name
    url = params.url

    engine = create_engine(
        f'postgresql://{user}:{password}@{host}:{port}/{db}')

    file_name = 'output.parquet'

    os.system(f"wget {url} -O {file_name}")
    parquet_file = pq.ParquetFile(file_name)

    flag = False

    for batch in parquet_file.iter_batches(batch_size=50000):
        t_start = time()

        table = pa.Table.from_batches([batch])
        df = table.to_pandas(split_blocks=True, self_destruct=True)

        if flag == False:
            df.to_sql(name='yellow_taxi_data',
                      con=engine, if_exists='replace')
            flag = True
        else:
            df.to_sql(name='yellow_taxi_data',
                      con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end-t_start))

    print("Finished ingesting data into the postgres database")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Ingest parquet data to Postgres')

    # user, password, host, port, database name, table name, url of the parquet file

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host name for postgres')
    parser.add_argument('--port', help='port number for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument(
        '--table_name', help='name of the table to insert into')
    parser.add_argument('--url', help='url of the parquet file')

    args = parser.parse_args()

    main(args)
