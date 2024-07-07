import os
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, String

print(os.getcwd())

path = os.path.dirname(os.getcwd()) + "\\projeto\\bda\\CSVFiles\\zones.csv"

df = pd.read_csv(path, encoding='windows-1252')

df.columns = ['zone_name', 'continent', 'area', 'zone', 'subzone', 'type', 'size',
              'controlled', 'min_req_level', 'min_rec_level',
              'max_rec_level', 'min_bot_level', 'max_bot_level']

df['zone_name'] = df['zone_name'].str.strip().str.lower()
df['continent'] = df['continent'].str.strip().str.lower()
df['area'] = df['area'].str.strip().str.lower()
df['zone'] = df['zone'].str.strip().str.lower()
df['subzone'] = df['subzone'].str.strip().str.lower()
df['type'] = df['type'].str.strip().str.lower()
df['controlled'] = df['controlled'].str.strip().str.lower()

df.drop_duplicates(subset='zone_name', inplace=True)

## Creating an Engine

username = 'root'
password = 'admin'
host = 'localhost'
port = '3306'
database_name = 'relationaldatabase'

db_url = f'mysql://{username}:{password}@{host}:{port}/{database_name}'

engine = create_engine(db_url)


with engine.connect() as connection:

    df.to_sql(name='zones', con=engine, if_exists='replace', index=False)

    inserted_row_count = connection.execute(text("SELECT COUNT(*) FROM zones")).fetchone()[0]

    # CHANGED ZONE DATA TYPE
    connection.execute(text('ALTER TABLE zones MODIFY COLUMN zone_name VARCHAR(255) CHARACTER SET latin1;'))

    # ADD PRIMARY KEY
    connection.execute(text('ALTER TABLE zones ADD PRIMARY KEY (zone_name);'))

    print(f'Inserted {inserted_row_count} rows.')
