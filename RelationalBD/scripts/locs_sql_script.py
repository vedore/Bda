import mysql.connector
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, String

path = os.path.dirname(os.getcwd()) + "\\projeto\\bda\\CSVFiles\\locations.csv"

df = pd.read_csv(path , encoding='windows-1252')

df = df[['Map_ID', 'Location_Type', 'Location_Name', 'Game_Version']]

df.columns = ['map_id', 'location_type', 'location_name', 'game_version']

df['location_type'] = df['location_type'].str.strip().str.lower()
df['location_name'] = df['location_name'].str.strip().str.lower()
df['game_version'] = df['game_version'].str.strip().str.lower()

df.drop_duplicates(subset='map_id', inplace=True)

## Creating an Engine

username = 'root'
password = 'admin'
host = 'localhost'
port = '3306'
database_name = 'relationaldatabase'

db_url = f'mysql://{username}:{password}@{host}:{port}/{database_name}'

engine = create_engine(db_url)

with engine.connect() as connection:

    df.to_sql(name='locations', con=engine, if_exists='replace', index=False)

    inserted_row_count = connection.execute(text("SELECT COUNT(*) FROM locations")).fetchone()[0]
    print(f'Inserted {inserted_row_count} rows.')

    # CHANGE DATA TYPES
    connection.execute(text('ALTER TABLE locations MODIFY COLUMN map_id INT;'))
    connection.execute(text('ALTER TABLE locations MODIFY COLUMN location_name VARCHAR(255) CHARACTER SET latin1;'))

    # ADD PRIMARY KEY
    connection.execute(text('ALTER TABLE locations ADD PRIMARY KEY (map_id);'))





