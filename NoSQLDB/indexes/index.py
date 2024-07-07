import pprint
import pandas as pd
import pymongo
from pymongo import MongoClient
import time

def call_indexes ():

    #Connect to MongoDB
    client = MongoClient('localhost', 27017)
    db = client['wow_database']
    my_collection = db['wow_data']

    # Indexing
    my_collection.create_index([('charclass', pymongo.ASCENDING)])
    my_collection.create_index([('level', pymongo.ASCENDING)])
    my_collection.create_index([('zone', pymongo.ASCENDING)])
    my_collection.create_index([('player', pymongo.ASCENDING)])