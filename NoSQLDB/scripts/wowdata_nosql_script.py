import os
import pandas as pd
from pymongo import MongoClient
import pprint

path = os.path.dirname(os.getcwd()) + "\\projeto\\bda\\CSVFiles\\wowah_data.csv"

# df = pd.read_csv(path, encoding='windows-1252', nrows=100000)
df = pd.read_csv(path, encoding='windows-1252')

df.columns = ['player', 'level', 'race', 'charclass', 'zone', 'guild', 'timestamp']

df.drop_duplicates(subset='player', inplace=True)

df['race'] = df['race'].str.strip().str.lower()
df['charclass'] = df["charclass"].str.strip().str.lower()
df['zone'] = df['zone'].str.strip().str.lower()

client = MongoClient('localhost', 27017)
db = client['wow_database']

db['wow_data'].drop()
col = db["wow_data"]

dict_file = df.to_dict(orient='records')

col.insert_many(dict_file)
