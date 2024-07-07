import mysql.connector
import pandas as pd
import pprint
import time
from indexes import index

# Establish the connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="relationaldatabase"
)

mycursor = mydb.cursor()

index.call_index()

# Start Time
StartTime = time.time()

# 1st Simple Query
first_Squery = """SELECT * 
    FROM wow_data 
    WHERE charclass = %s AND level BETWEEN %s AND %s
"""

params = ('Hunter', 20, 60)
mycursor.execute(first_Squery, params)

counter = 0
for row in mycursor.fetchall():
    counter += 1


# 2nd Simple Query
second_Squery = """SELECT * 
    FROM wow_data 
    WHERE level > %s
"""

param = (50,)
mycursor.execute(second_Squery, param)

counter = 0
for row in mycursor.fetchall():
    counter += 1


# 1st Complex Query
first_Cquery = """SELECT *
    FROM wow_data 
    JOIN zones ON wow_data.zone = zones.zone_name
    WHERE wow_data.zone = %s 
"""

param = ('orgrimmar',)
mycursor.execute(first_Cquery, param)

columns = [desc[0] for desc in mycursor.description]
counter = 0

for row in mycursor.fetchall():
    data_dict = dict(zip(columns, row))
    counter += 1


# 2nd Complex Query
second_Cquery = """SELECT wow_data.player, wow_data.level, zones.zone_name, locations.location_name
    FROM wow_data
    JOIN zones ON wow_data.zone = zones.zone_name
    JOIN locations ON zones.zone_name = locations.location_name
    WHERE zones.zone_name = 'alterac valley' AND wow_data.level > 60
"""

mycursor.execute(second_Cquery)
columns = [desc[0] for desc in mycursor.description]
counter = 0

for row in mycursor.fetchall():
    data_dict = dict(zip(columns, row))
    counter += 1


# Insert Query

#Zones
myquery_zones = """INSERT INTO zones (zone_name, continent, area, subzone, type, size, controlled, min_req_level, 
                min_rec_level, max_rec_level, min_bot_level, max_bot_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE zone_name = VALUES(zone_name);
"""
zone_values = ('portugal', 'europa', 'west europe', 'lisboa', 'city', '10000', 'pelos portugueses', '0', '0', '100', '100', '100')

mycursor.execute(myquery_zones, zone_values)
mydb.commit()


#WowData
myquery_wowdata = """INSERT INTO wow_data (charclass, guild, level, player, race, timestamp, zone)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE player = VALUES(player);
"""
wowdata_values = ('warrior', 4, 50, 90593, 'human', '12/31/08 20:59:51', 'portugal')

mycursor.execute(myquery_wowdata, wowdata_values)


#Locs
myquery_locs = """INSERT INTO locations (map_id, location_type, location_name, game_version)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE map_id = VALUES(map_id);
"""
locs_values = (1000, 'continent', 'portugal', 'tuga')

mycursor.execute(myquery_locs, locs_values)


#LocCords
myquery_loccords = """INSERT INTO location_coords (location_name, map_id, x_coord, y_coord, z_coord)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE location_name = VALUES(location_name);
"""
loccords_values = ('portugal', 0, 0.1, 0.2, 3)

mycursor.execute(myquery_loccords, loccords_values)
mydb.commit()

# Update Query (with parameterized query)
update_query = """UPDATE wow_data 
    SET level = %s
    WHERE player = %s 
"""

update_values = (4, 90575)
mycursor.execute(update_query, update_values)
mydb.commit()


# End Time
EndTime = time.time()
ExecutionTime = EndTime - StartTime
print(f"Optimized Execution Time: {ExecutionTime} seconds")
