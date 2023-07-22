#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import pandas as pd
import mysql.connector
import glob
import math


# In[ ]:


green_taxi_directory = 'E:\ITI\Case Study - TAXI\high\\'


# In[ ]:



# Establish a connection to MySQL
cnx = mysql.connector.connect(
    host='localhost',
    port=3307,
    user='ama',
    password='ama',
    database="all_data"
)

# Create a cursor to execute SQL queries
cursor = cnx.cursor()


# In[ ]:


def read_data(directory):
    all_files = glob.glob(directory + "*.parquet")
    if not all_files:
        raise ValueError("No Parquet files found in the directory: " + directory)
    df_list = []
    for file in all_files:
        df = pd.read_parquet(file)
        df_list.append(df)
    return pd.concat(df_list)

df=read_data(green_taxi_directory)


# In[ ]:


# Specify the columns to be dropped
columns_to_drop = ['hvfhs_license_num', 'dispatching_base_num', 'originating_base_num','request_datetime','on_scene_datetime',
                 'shared_request_flag','shared_match_flag',"access_a_ride_flag","wav_request_flag","wav_match_flag" ]

# Drop the specified columns from the DataFrame
df.drop(columns=columns_to_drop, inplace=True)


# In[ ]:


# Extract hour
df['hour'] = df['pickup_datetime'].dt.hour

# Extract day
df['day'] = df['pickup_datetime'].dt.day

# Extract month
df['month'] = df['pickup_datetime'].dt.month

# Extract year
df['year'] = df['pickup_datetime'].dt.year

# Extract day name
df['day_name'] = df['pickup_datetime'].dt.day_name()

df.fillna(0, inplace=True)


# In[ ]:


#4)Change Trip distance from mile to km and approximate to nearest decimal place
df["trip_miles"] = round(df["trip_miles"] * 1.60934, 2)
df = df[df["base_passenger_fare"] >= 0]


# In[ ]:


# Define the batch size
batch_size = 10000
total_rows = len(df)
num_batches = math.ceil(total_rows / batch_size)
table_name = 'hvfhs_table'

# Get the columns and placeholders for the query
columns = ','.join(df.columns)
placeholders = ','.join(['%s'] * len(df.columns))

# Define the query
query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

# Get the total number of rows
total_rows = len(df)
num_batches = math.ceil(total_rows / batch_size)

# Insert rows in batches
for i in range(num_batches):
    start_index = i * batch_size
    end_index = min((i + 1) * batch_size, total_rows)
    batch_values = [tuple(row) for row in df.values[start_index:end_index]]
    
    cursor.executemany(query, batch_values)
    cnx.commit()
    print(f"Inserted rows {start_index + 1} to {end_index}.")

# Close the cursor and MySQL connection
cursor.close()
cnx.close()


# In[ ]:




