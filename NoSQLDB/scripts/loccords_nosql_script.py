import os
import pandas as pd
from pymongo import MongoClient
import pprint

path = os.path.dirname(os.getcwd()) + "\\projeto\\bda\\CSVFiles\\location_coords.csv"

df = pd.read_csv(path, encoding='windows-1252')

df.columns = ['location_name', 'map_id', 'x_coord', 'y_coord', 'z_coord']

df['location_name'] = df['location_name'].str.strip().str.lower()

df.drop_duplicates(subset='location_name', inplace=True)

client = MongoClient('localhost', 27017)
db = client['wow_database']

db["location_coords"].drop()
col = db["location_coords"]

dict_file = df.to_dict(orient='records')

col.insert_many(dict_file)


