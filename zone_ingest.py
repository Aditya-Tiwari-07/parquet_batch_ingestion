import pandas as pd
from sqlalchemy import create_engine
import os

engine = create_engine(
    "postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/{POSTGRES_DB}".format(**os.environ))

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
file_name = "output.csv"

os.system(f"wget {url} -O {file_name}")
df_zones = pd.read_csv(f'{file_name}')
df_zones['service_zone'].fillna('None', inplace=True)

df_zones.to_sql(name='zones', con=engine, if_exists='replace')
