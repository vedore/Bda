import pprint
import pandas as pd
import pymongo
from pymongo import MongoClient
import time

client = MongoClient('localhost', 27017)
db = client['wow_database']

my_collection = db['wow_data']

# Start Time

StartTime = time.time()

# 1º Simple Query
# myquery = {'charclass': {'$eq': 'hunter'}, 'level': {'$gte': 20, '$lte': 60}}}

myquery = {'$and': [{'charclass': {'$eq': 'hunter'}}, {'level': {'$gte': 20, '$lte': 60}}]}

responseDoc = my_collection.find(myquery)

counter = 0

for doc in responseDoc:
    counter += 1


# 2º Simple Query
myquery = {'level': {'$gt': 50}}

responseDoc = my_collection.find(myquery)

counter = 0

for doc in responseDoc:
    counter += 1


# 1º Complex Query
myquery

match = {
    "$match": {
        "zone": "orgrimmar"
    }
}

zone_lockup_location_name = {
    "$lookup": {
        "from": "zones",
        "localField": "zone",
        "foreignField": "zone_name",
        "as": "loca",
    },
}

unwind = {
    "$unwind": "$loca",
}

zone_limit = {"$limit": 5}

pipeline = [
    match,
    zone_lockup_location_name,
    unwind,
    zone_limit,
    zone_limit
]

responseDoc = my_collection.aggregate(pipeline)

counter = 0

for doc in responseDoc:
    counter += 1


#2ª Complex Query

match_stage = {
    "$match": {
        "level": { "$gt": 60 }
    }
}

zone_lookup_stage = {
    "$lookup": {
        "from": "zones",
        "localField": "zone",
        "foreignField": "zone_name",
        "as": "zoneData"
    }
}

unwind_stage = {
    "$unwind": "$zoneData"
}

location_lookup_stage = {
    "$lookup": {
        "from": "locations",
        "localField": "zoneData.zone_name",
        "foreignField": "location_name",
        "as": "locationData"
    }
}

unwind_location_stage = {
    "$unwind": "$locationData"
}

project_stage = {
    "$project": {
        "player": 1,
        "level": 1,
        "zoneData.zone_name": 1,
        "locationData.location_name": 1,
    }
}

pipeline = [
    match_stage,
    zone_lookup_stage,
    unwind_stage,
    location_lookup_stage,
    unwind_location_stage,
    project_stage,
]

response_cursor = my_collection.aggregate(pipeline)

counter = 0

for doc in response_cursor:
    counter += 1

# Update One Value Query

myquery = {'player': 90575}
newValues = {'$set': {'level': 4}}

responseDoc = my_collection.update_one(myquery, newValues)


# Insert One Value Query

myquery_wowdata = {'charclass': 'warrior', 'guild': 4, 'level': 50, 'player': 90576, 'race': 'human',
           'timestamp': '12/31/08 20:59:51', 'zone': 'portugal'}

myquery_zones = {'zone_name': 'portugal', 'continent': 'europa', 'area': 'west europe', 'zone': 'portugal', 'subzone': 'lisboa',
                 'type': 'city', 'size': '10000','controlled': 'pelos portugueses', 'min_req_level': '0', 'min_rec_level': '0',
                 'max_rec_level': '100', 'min_bot_level': '100', 'max_bot_level': '100'}

myquery_locs = {'map_id': 0, 'location_type': "continent", 'location_name': "portugal", 'game_version': "tuga"}

myquery_loccords = {"location_name": "portugal", 'map_id': 0, 'x_coord': 0.1, 'y_coord': 0.2, 'z_coord': 3}

responseDoc = my_collection.insert_one(myquery_wowdata)
responseDoc = my_collection.insert_one(myquery_zones)
responseDoc = my_collection.insert_one(myquery_locs)
responseDoc = my_collection.insert_one(myquery_loccords)

# End Time
EndTime = time.time()
ExecutionTime = EndTime - StartTime
print(f"Normal Execution Time: {ExecutionTime} seconds")

