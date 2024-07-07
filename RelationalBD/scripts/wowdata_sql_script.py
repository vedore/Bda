import os
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, Float, String

path = os.path.dirname(os.getcwd()) + "\\projeto\\bda\\CSVFiles\\wowah_data.csv"

df = pd.read_csv(path, encoding='windows-1252')

df.columns = ['player', 'level', 'race', 'charclass', 'zone', 'guild', 'timestamp']


df['race'] = df['race'].str.strip().str.lower()
df['charclass'] = df['charclass'].str.strip().str.lower()
df['zone'] = df['zone'].str.strip().str.lower()
df['timestamp'] = df['timestamp'].str.strip().str.lower()

df.drop_duplicates(subset='player', inplace=True)

print(df)

## Creating an Engine

username = 'root'
password = 'admin'
host = 'localhost'
port = '3306'
database_name = 'relationaldatabase'

db_url = f'mysql://{username}:{password}@{host}:{port}/{database_name}'

engine = create_engine(db_url)

with engine.connect() as connection:

    df.to_sql(name='wow_data', con=engine, if_exists='replace', index=False)

    inserted_row_count = connection.execute(text("SELECT COUNT(*) FROM wow_data")).fetchone()[0]

    # ADD PRIMARY KEY
    connection.execute(text('ALTER TABLE wow_data ADD PRIMARY KEY (player);'))

    # CHANGED ZONE DATA TYPE
    connection.execute(text('ALTER TABLE wow_data MODIFY COLUMN zone VARCHAR(255) CHARACTER SET latin1;'))

    # ADD FOREIGN KEY
    #connection.execute(text('ALTER TABLE wow_data ADD FOREIGN KEY (zone) REFERENCES zones(zone_name) ON DELETE NO ACTION;'))

    print(f'Inserted {inserted_row_count} rows.')
