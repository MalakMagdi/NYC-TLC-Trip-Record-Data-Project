#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import mysql.connector
import glob
import math
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

yellow_taxi_directory = 'E:\ITI\Case Study - TAXI\yellow\\'

def read_data(directory):
    all_files = glob.glob(directory + "*.parquet")
    if not all_files:
        raise ValueError("No Parquet files found in the directory: " + directory)
    df_list = []
    for file in all_files:
        df = pd.read_parquet(file)
        df_list.append(df)
    return pd.concat(df_list)



# In[3]:


df = read_data(yellow_taxi_directory)

df.fillna(0, inplace=True)
df['passenger_count'] = df['passenger_count'].astype(int)
df['RatecodeID'] = df['RatecodeID'].astype(int)

# 1) Change Payment type from numbers to human readable form
payment_type_mapping = {
    0: "Not defined",
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip"
}
df["payment_type"] = df["payment_type"].replace(payment_type_mapping)

# 2) Change RateID from numbers to human readable form
rate_code_mapping = {
    0: "Not defined",
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride"
}
df["RatecodeID"] = df["RatecodeID"].replace(rate_code_mapping)

# 4) Change Trip distance from mile to km and approximate to nearest decimal place
df["trip_distance"] = round(df["trip_distance"] * 1.60934, 2)
df = df[df["fare_amount"] >= 0]

# Extract date and time components
df['hour'] = df['tpep_pickup_datetime'].dt.hour
df['day'] = df['tpep_pickup_datetime'].dt.day
df['month'] = df['tpep_pickup_datetime'].dt.month
df['year'] = df['tpep_pickup_datetime'].dt.year
df['day_name'] = df['tpep_pickup_datetime'].dt.day_name()

df.fillna(0, inplace=True)


# In[ ]:


# Define the batch size
batch_size = 100000
total_rows = len(df)
num_batches = math.ceil(total_rows / batch_size)
table_name = 'yellow'

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




