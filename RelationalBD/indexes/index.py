import mysql.connector
import pandas as pd
import pprint
import time

def call_index():

    # Establish the connection
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="admin",
        database="relationaldatabase"
    )

    mycursor = mydb.cursor()

    # Add Indexes
    #index_query_wow_data = "CREATE INDEX charclass_level_index ON wow_data (charclass(255), level)"
    #index_query_zones = "CREATE INDEX zone_name_index ON zones (zone_name)"

    #mycursor.execute(index_query_wow_data)
    #mycursor.execute(index_query_zones)
    #mydb.commit()