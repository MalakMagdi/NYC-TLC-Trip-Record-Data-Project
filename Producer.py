#!/usr/bin/env python
# coding: utf-8

# In[7]:


from confluent_kafka import Producer
import pandas as pd
import time
import json
import glob
import random

# Kafka configuration
bootstrap_servers = 'localhost:9092'  # Replace with your Kafka bootstrap servers

# Create Kafka Producer
producer = Producer({'bootstrap.servers': bootstrap_servers})


# Define the directory paths for each trip type data

yellow_taxi_directory = 'E:\ITI\Case Study - TAXI\yellow\\'
green_taxi_directory = 'E:\ITI\Case Study - TAXI\green\\'
#forhire_vehicle_directory = 'E:\ITI\Case Study - TAXI\\fhv\\'




# Define the topic names for each trip type

yellow_taxi_topic = 'yellow_taxi_topic'
green_taxi_topic = 'green_taxi_topic'
#forhire_vehicle_topic = 'forhire_vehicle_topic'



# In[8]:



# Function to push data to Kafka
def push_to_kafka(topic, data):
    for record in data:
        producer.produce(topic, json.dumps(record, default=str).encode('utf-8'))
    producer.flush()


def read_data(directory):
    all_files = glob.glob(directory + "*.parquet")
    if not all_files:
        raise ValueError("No Parquet files found in the directory: " + directory)
    df_list = []
    for file in all_files:
        df = pd.read_parquet(file)
        df_list.append(df)
    data = pd.concat(df_list)
    return data


# Read and sort the data

yellow_taxi_data = read_data(yellow_taxi_directory)
green_taxi_data = read_data(green_taxi_directory)
#fhv_taxi_data = read_data(forhire_vehicle_directory)


# Start the simulation

while True:
    # Get a random number of records to push for each trip type
    
    yellow_taxi_records = random.randint(100 , 500)
    green_taxi_records = random.randint(100 , 500)
    #fhv_taxi_records = random.randint(100 , 500)

    
    
    # Get the records to push for each trip type
    
    yellow_taxi_pushed_data = yellow_taxi_data.head(yellow_taxi_records).to_dict(orient='records')
    green_taxi_pushed_data = green_taxi_data.head(green_taxi_records).to_dict(orient='records')
    #fhv_taxi_pushed_data = fhv_taxi_data.head(fhv_taxi_records).to_dict(orient='records')

    
    # Process and push the data for each trip type to their respective topics
    
    push_to_kafka(yellow_taxi_topic, yellow_taxi_pushed_data)
    push_to_kafka(green_taxi_topic, green_taxi_pushed_data)
    #push_to_kafka(forhire_vehicle_topic, fhv_taxi_pushed_data) 

    
    
    # Remove the pushed data from the dataframe
    
    yellow_taxi_data = yellow_taxi_data.iloc[yellow_taxi_records:]
    green_taxi_data = green_taxi_data.iloc[green_taxi_records:]
    #fhv_taxi_data = fhv_taxi_data.iloc[fhv_taxi_records:]

    
    
    # Wait for the simulation interval
    time.sleep(10)







