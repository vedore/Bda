import mysql.connector
import pandas as pd
import pprint
import time

# Create Database

#mydb = mysql.connector.connect(
#    host="localhost",
#    user="root",
#    password="admin"
#)

# Create a cursor object to interact with the database
#mycursor = mydb.cursor()

#mycursor.execute(f"CREATE DATABASE relationaldatabase")

# Establish the connection

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="admin",
    database="relationaldatabase"
)

mycursor = mydb.cursor()

# Start Time

StartTime = time.time()

# 1st Simple Query

first_Squery = """SELECT * 
    FROM wow_data 
    WHERE charclass = 'Hunter' AND level >= 20 AND level <= 60
"""

mycursor.execute(first_Squery)
counter = 0

for row in mycursor.fetchall():
    counter += 1


# 2nd Simple Query

second_Squery = """SELECT * 
    FROM wow_data 
    WHERE level > 50
"""

mycursor.execute(second_Squery)
counter = 0

for row in mycursor.fetchall():
    counter += 1


# 1st Complex Query

first_Cquery = """SELECT *
    FROM wow_data 
    JOIN zones ON wow_data.zone = zones.zone_name
    WHERE wow_data.zone = 'orgrimmar' 
"""

mycursor.execute(first_Cquery)
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

myquery_zones = """INSERT INTO zones (zone_name, continent, area, subzone, type, size, controlled, min_req_level, 
                min_rec_level, max_rec_level, min_bot_level, max_bot_level)
                VALUES ('portugal', 'europa', 'west europe', 'lisboa', 'city', '10000', 'pelos portugueses', '0', '0', '100', '100', '100')
                ON DUPLICATE KEY UPDATE zone_name = VALUES(zone_name);
"""

mycursor.execute(myquery_zones)
mydb.commit()

myquery_wowdata = """INSERT INTO wow_data (charclass, guild, level, player, race, timestamp, zone)
                VALUES ('warrior', 4, 50, 90589, 'human', '12/31/08 20:59:51', 'portugal')
                ON DUPLICATE KEY UPDATE player = VALUES(player);
"""


myquery_locs = """INSERT INTO locations (map_id, location_type, location_name, game_version)
                VALUES (1000, 'continent', 'portugal', 'tuga')
                ON DUPLICATE KEY UPDATE map_id = VALUES(map_id);
"""

myquery_loccords = """INSERT INTO location_coords (location_name, map_id, x_coord, y_coord, z_coord)
                    VALUES ('portugal', 0, 0.1, 0.2, 3)
                    ON DUPLICATE KEY UPDATE location_name = VALUES(location_name);
"""


mycursor.execute(myquery_wowdata)
mycursor.execute(myquery_locs)
mycursor.execute(myquery_loccords)
mydb.commit()


# Update Query

update_query = """UPDATE wow_data 
    SET level = 4
    WHERE player = 90575 
"""

mycursor.execute(update_query)
mydb.commit()


# End Time

EndTime = time.time()
ExecutionTime = EndTime - StartTime
print(f"Normal Execution Time: {ExecutionTime} seconds")


#se for necessário apagar as tabelas

#dropWowData = "DROP TABLE IF EXISTS wow_data"
#dropZones = "DROP TABLE IF EXISTS zones"
#dropLocations = "DROP TABLE IF EXISTS locations"
#dropLocationsCoords = "DROP TABLE IF EXISTS location_coordinates"

# Execute Drop Table Statements
#mycursor.execute(dropWowData)
#mycursor.execute(dropZones)
#mycursor.execute(dropLocationse)
#mycursor.execute(dropLocationsCoords)
#mydb.commit()



