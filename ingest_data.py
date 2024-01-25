#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from time import time
from sqlalchemy import create_engine
import pyarrow.parquet as pq
import pyarrow as pa


def main():
    user = os.environ['POSTGRES_USER']
    password = os.environ['POSTGRES_PASSWORD']
    host = os.environ['HOST']
    port = os.environ['PORT']
    db = os.environ['POSTGRES_DB']
    table_name = os.environ['TABLE_NAME']
    url = os.environ['URL']

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
            df.to_sql(name=f"{table_name}",
                      con=engine, if_exists='replace')
            flag = True
        else:
            df.to_sql(name=f"{table_name}",
                      con=engine, if_exists='append')

        t_end = time()

        print('inserted another chunk, took %.3f second' % (t_end-t_start))

    print("Finished ingesting data into the postgres database")


if __name__ == '__main__':
    main()
