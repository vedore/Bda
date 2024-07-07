import mysql.connector
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, String


path = os.path.dirname(os.getcwd()) + "\\projeto\\bda\\CSVFiles\\location_coords.csv" #apagar "\\projeto\\bda"

df = pd.read_csv(path , encoding='windows-1252')

df.columns = ['location_name', 'map_id', 'x_coord', 'y_coord', 'z_coord']

df['location_name'] = df['location_name'].str.strip().str.lower()

df.drop_duplicates(subset='location_name', inplace=True)

## Creating an Engine

username = 'root'
password = 'admin'
host = 'localhost'
port = '3306'
database_name = 'relationaldatabase'

db_url = f'mysql://{username}:{password}@{host}:{port}/{database_name}'

engine = create_engine(db_url)

with engine.connect() as connection:

    df.to_sql(name='location_coords', con=engine, if_exists='replace', index=False)

    inserted_row_count = connection.execute(text("SELECT COUNT(*) FROM location_coords")).fetchone()[0]
    print(f'Inserted {inserted_row_count} rows.')

    # CHANGED location_name DATA TYPE
    connection.execute(text('ALTER TABLE location_coords MODIFY COLUMN location_name VARCHAR(255) CHARACTER SET latin1;'))
    connection.execute(text('ALTER TABLE location_coords MODIFY COLUMN map_id INT;'))

    # ADD PRIMARY KEY
    connection.execute(text('ALTER TABLE location_coords ADD PRIMARY KEY (location_name);'))





