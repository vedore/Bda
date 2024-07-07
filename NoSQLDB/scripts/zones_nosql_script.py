import os
import pandas as pd
from pymongo import MongoClient
import pprint

path = os.path.dirname(os.getcwd()) + "\\projeto\\bda\\CSVFiles\\zones.csv"

df = pd.read_csv(path, encoding='windows-1252', nrows=20000)

df.columns = ['zone_name', 'continent', 'area', 'zone', 'subzone', 'type', 'size',
              'controlled', 'min_req_level', 'min_rec_level',
              'max_rec_level', 'min_bot_level', 'max_bot_level']

df.drop_duplicates(subset='zone_name', inplace=True)

df['zone_name'] = df['zone_name'].str.strip().str.lower()
df['continent'] = df['continent'].str.strip().str.lower()
df['area'] = df['area'].str.strip().str.lower()
df['zone'] = df['zone'].str.strip().str.lower()
df['subzone'] = df['subzone'].str.strip().str.lower()
df['type'] = df['type'].str.strip().str.lower()
df['controlled'] = df['controlled'].str.strip().str.lower()

client = MongoClient('localhost', 27017)
db = client['wow_database']

db['zones'].drop()
col = db["zones"]

dict_file = df.to_dict(orient='records')

col.insert_many(dict_file)
