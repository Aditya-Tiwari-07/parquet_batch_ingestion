import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')

engine = create_engine(
    "postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/{POSTGRES_DB}".format(**os.environ))

url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
file_name = "output.csv"

os.system(f"wget {url} -O {file_name}")
df_zones = pd.read_csv(f'{file_name}')
df_zones['service_zone'].fillna('None', inplace=True)

df_zones.to_sql(name='zones', con=engine, if_exists='replace')
