import pprint
import pandas as pd
import pymongo
from pymongo import MongoClient
from indexes import index
import time

# Connect to MongoDB
client = MongoClient('localhost', 27017)
db = client['wow_database']
my_collection = db['wow_data']

index.call_indexes()

# Start Time
StartTime = time.time()

# 1º Simple Query
myquery = {'$and': [{'charclass': 'hunter'}, {'level': {'$gte': 20, '$lte': 60}}]}
responseDoc = my_collection.find(myquery)

# Use Explain to Analyze Queries

counter = 0
for doc in responseDoc:
    counter += 1


# 2º Simple Query
myquery = {'level': {'$gt': 50}}
responseDoc = my_collection.find(myquery)

# Use Explain to Analyze Queries

counter = 0
for doc in responseDoc:
    counter += 1


# 1º Complex Query
match = {"$match": {"zone": "orgrimmar"}}
zone_lockup_location_name = {
    "$lookup": {
        "from": "zones",
        "localField": "zone",
        "foreignField": "zone_name",
        "as": "loca",
    },
}
unwind = {"$unwind": "$loca"}
zone_limit = {"$limit": 5}
pipeline = [match, zone_lockup_location_name, unwind, zone_limit]



responseDoc = my_collection.aggregate(pipeline)
counter = 0
for doc in responseDoc:
    counter += 1

#print("Number of docs:", counter)


# 2º Complex Query
pipeline = [
    {
        "$match": {
            "level": {"$gt": 60}
        }
    },
    {
        "$lookup": {
            "from": "zones",
            "localField": "zone",
            "foreignField": "zone_name",
            "as": "zoneData"
        }
    },
    {
        "$unwind": "$zoneData"
    },
    {
        "$lookup": {
            "from": "locations",
            "localField": "zoneData.zone_name",
            "foreignField": "location_name",
            "as": "locationData"
        }
    },
    {
        "$unwind": "$locationData"
    },
    {
        "$project": {
            "player": 1,
            "level": 1,
            "zoneData.zone_name": 1,
            "locationData.location_name": 1,
        }
    }
]

response_cursor = my_collection.aggregate(pipeline)

# Iterate over the cursor and pprint each document
counter = 0
for doc in response_cursor:
    counter += 1




# Update One Value Query with Upsert
myquery = {'player': 90575}
newValues = {'$set': {'level': 4}}
responseDoc = my_collection.update_one(myquery, newValues, upsert=True)


# Bulk Insert
insertData = [
    {'charclass': 'warrior', 'guild': 4, 'level': 50, 'player': 90576, 'race': 'human',
     'timestamp': '12/31/08 20:59:51', 'zone': 'portugal'},
    {'zone_name': 'portugal', 'continent': 'europa', 'area': 'west europe', 'zone': 'portugal', 'subzone': 'lisboa',
     'type': 'city', 'size': '10000', 'controlled': 'pelos portugueses', 'min_req_level': '0', 'min_rec_level': '0',
     'max_rec_level': '100', 'min_bot_level': '100', 'max_bot_level': '100'},
    {'map_id': 0, 'location_type': "continent", 'location_name': "portugal", 'game_version': "tuga"},
    {"location_name": "portugal", 'map_id': 0, 'x_coord': 0.1, 'y_coord': 0.2, 'z_coord': 3}
]

# Use insert_many for Bulk Insert
responseDoc = my_collection.insert_many(insertData)


# End Time
EndTime = time.time()
ExecutionTime = EndTime - StartTime
print(f"Optimized Execution Time: {ExecutionTime} seconds")