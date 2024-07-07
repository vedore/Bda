import os
import pandas as pd
from pymongo import MongoClient
import pprint

path = os.path.dirname(os.getcwd()) + "\\projeto\\bda\\CSVFiles\\locations.csv"

df = pd.read_csv(path, encoding='windows-1252')

df = df[['Map_ID', 'Location_Type', 'Location_Name', 'Game_Version']]

df.columns = ['map_id', 'location_type', 'location_name', 'game_version']

df['location_type'] = df['location_type'].str.strip().str.lower()
df['location_name'] = df['location_name'].str.strip().str.lower()
df['game_version'] = df['game_version'].str.strip().str.lower()

client = MongoClient('localhost', 27017)
db = client['wow_database']

db['locations'].drop()
col = db["locations"]

dict_file = df.to_dict(orient='records')

col.insert_many(dict_file)
