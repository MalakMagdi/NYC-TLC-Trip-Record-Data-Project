from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *



# Create a SparkSession
spark = SparkSession.builder.appName("KafkaStreamReader").getOrCreate()




# Define the Kafka broker and topic details
kafka_broker = "host.docker.internal:29092"




# Define the MySQL database connection details
mysql_url = "jdbc:mysql://host.docker.internal:3307/stage"
mysql_properties = {
    "user": "ama",
    "password": "ama",
    "driver": "com.mysql.cj.jdbc.Driver"
}





# Define the schema for the JSON data
yellowSchema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", FloatType()),
    StructField("RatecodeID", FloatType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", FloatType()),
    StructField("extra", FloatType()),
    StructField("mta_tax", FloatType()),
    StructField("tip_amount", FloatType()),
    StructField("tolls_amount", FloatType()),
    StructField("improvement_surcharge", FloatType()),
    StructField("total_amount", FloatType()),
    StructField("congestion_surcharge", FloatType()),
    StructField("airport_fee", FloatType())
])


greenSchema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("lpep_pickup_datetime", TimestampType()),
    StructField("lpep_dropoff_datetime", TimestampType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("RatecodeID", FloatType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("passenger_count", FloatType()),
    StructField("trip_distance", FloatType()),
    StructField("fare_amount", FloatType()),
    StructField("extra", FloatType()),
    StructField("mta_tax", FloatType()),
    StructField("tip_amount", FloatType()),
    StructField("tolls_amount", FloatType()),
    StructField("ehail_fee", FloatType()),
    StructField("improvement_surcharge", FloatType()),
    StructField("total_amount", FloatType()),
    StructField("payment_type", FloatType()),
    StructField("trip_type", FloatType()),
    StructField("congestion_surcharge", FloatType())
])

# fvhSchema = StructType([
#     StructField("dispatching_base_num", StringType()),
#     StructField("pickup_datetime", TimestampType()),
#     StructField("dropOff_datetime", TimestampType()),
#     StructField("PUlocationID", FloatType()),
#     StructField("DOlocationID", FloatType()),
#     StructField("SR_Flag", StringType()),
#     StructField("Affiliated_base_number", StringType())
# ])



yellow_taxi_topic = 'yellow_taxi_topic'
green_taxi_topic = 'green_taxi_topic'
#forhire_vehicle_topic = 'forhire_vehicle_topic'



# Read data from Kafka topic as a streaming DataFrame
yellow_streaming_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", yellow_taxi_topic) \
    .option("startingOffsets", "earliest") \
    .load()

green_streaming_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", green_taxi_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# fvh_streaming_df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", kafka_broker) \
#     .option("subscribe", forhire_vehicle_topic) \
#     .option("startingOffsets", "earliest") \
#     .load()






# Convert the value column (containing JSON data) to string

yellow_streaming_df = yellow_streaming_df.selectExpr("CAST(value AS STRING)")
green_streaming_df = green_streaming_df.selectExpr("CAST(value AS STRING)")
#fvh_streaming_df = fvh_streaming_df.selectExpr("CAST(value AS STRING)")



# Parse the JSON data and apply the defined schema

parsed_yellow_df = yellow_streaming_df.select(from_json(yellow_streaming_df.value,yellowSchema).alias("data")).select("data.*")
parsed_green_df = green_streaming_df.select(from_json(green_streaming_df.value,greenSchema).alias("data")).select("data.*")
# parsed_fvh = fvh_streaming_df.select(from_json(fvh_streaming_df.value,fvhSchema).alias("data")).select("data.*")




#-----------------------------------------------------------------------------------------------------------------------------------------
#-------------------------------------------------------------------------------------------------------------------------
#Yellow_Transformations:
#-----------------------
#-----------------------


# Fill missing values with 0
parsed_yellow_df = parsed_yellow_df.fillna(0)

# Change data types
parsed_yellow_df = parsed_yellow_df.withColumn("passenger_count", parsed_yellow_df["passenger_count"].cast(IntegerType()))
parsed_yellow_df = parsed_yellow_df.withColumn("RatecodeID", parsed_yellow_df["RatecodeID"].cast(IntegerType()))


# Replace payment type values with human-readable form
parsed_yellow_df = parsed_yellow_df.withColumn("payment_type", when(col("payment_type") == 0, "Not defined")
                   .when(col("payment_type") == 1, "Credit card")
                   .when(col("payment_type") == 2, "Cash")
                   .when(col("payment_type") == 3, "No charge")
                   .when(col("payment_type") == 4, "Dispute")
                   .when(col("payment_type") == 5, "Unknown")
                   .when(col("payment_type") == 6, "Voided trip")
                   .otherwise(col("payment_type")))



# Replace rate code values with human-readable form
parsed_yellow_df = parsed_yellow_df.withColumn("RatecodeID", when(col("RatecodeID") == 0, "Not defined")
                   .when(col("RatecodeID") == 1, "Standard rate")
                   .when(col("RatecodeID") == 2, "JFK")
                   .when(col("RatecodeID") == 3, "Newark")
                   .when(col("RatecodeID") == 4, "Nassau or Westchester")
                   .when(col("RatecodeID") == 5, "Negotiated fare")
                   .when(col("RatecodeID") == 6, "Group ride")
                   .otherwise(col("RatecodeID")))


# Convert trip distance from miles to km and round to nearest decimal place
parsed_yellow_df = parsed_yellow_df.withColumn("trip_distance", round(parsed_yellow_df["trip_distance"] * 1.60934, 2))

# Filter out rows with negative fare amount
parsed_yellow_df = parsed_yellow_df.filter(col("fare_amount") >= 0)

# Extract date and time components
# Extract hour
parsed_yellow_df = parsed_yellow_df.withColumn("hour", hour(parsed_yellow_df["tpep_pickup_datetime"]))

# Extract day
parsed_yellow_df = parsed_yellow_df.withColumn("day", dayofmonth(parsed_yellow_df["tpep_pickup_datetime"]))

# Extract month
parsed_yellow_df = parsed_yellow_df.withColumn("month", month(parsed_yellow_df["tpep_pickup_datetime"]))

# Extract year
parsed_yellow_df = parsed_yellow_df.withColumn("year", year(parsed_yellow_df["tpep_pickup_datetime"]))

# Extract day name
parsed_yellow_df = parsed_yellow_df.withColumn("day_name", date_format(parsed_yellow_df["tpep_pickup_datetime"], "EEEE"))


# Fill any remaining missing values with 0
parsed_yellow_df = parsed_yellow_df.fillna(0)

#-----------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------
#green_Transformations:
#-----------------------
#-----------------------

# Fill missing values with 0
parsed_green_df = parsed_green_df.fillna(0)

# Change column types
parsed_green_df = parsed_green_df.withColumn("payment_type", parsed_green_df["payment_type"].cast(IntegerType()))
parsed_green_df = parsed_green_df.withColumn("RatecodeID", parsed_green_df["RatecodeID"].cast(IntegerType()))
parsed_green_df = parsed_green_df.withColumn("passenger_count", parsed_green_df["passenger_count"].cast(IntegerType()))
parsed_green_df = parsed_green_df.withColumn("trip_type", parsed_green_df["trip_type"].cast(IntegerType()))


# Replace payment type values with human-readable form
parsed_green_df = parsed_green_df.withColumn("payment_type", when(col("payment_type") == 0, "Not defined")
                   .when(col("payment_type") == 1, "Credit card")
                   .when(col("payment_type") == 2, "Cash")
                   .when(col("payment_type") == 3, "No charge")
                   .when(col("payment_type") == 4, "Dispute")
                   .when(col("payment_type") == 5, "Unknown")
                   .when(col("payment_type") == 6, "Voided trip")
                   .otherwise(col("payment_type")))



# Replace rate code values with human-readable form
parsed_green_df = parsed_green_df.withColumn("RatecodeID", when(col("RatecodeID") == 0, "Not defined")
                   .when(col("RatecodeID") == 1, "Standard rate")
                   .when(col("RatecodeID") == 2, "JFK")
                   .when(col("RatecodeID") == 3, "Newark")
                   .when(col("RatecodeID") == 4, "Nassau or Westchester")
                   .when(col("RatecodeID") == 5, "Negotiated fare")
                   .when(col("RatecodeID") == 6, "Group ride")
                   .otherwise(col("RatecodeID")))


# Replace trip type values with human-readable form
parsed_green_df = parsed_green_df.withColumn("trip_type", when(col("trip_type") == 0, "Not defined")
                   .when(col("trip_type") == 1, "Street-hail")
                   .when(col("trip_type") == 2, "Dispatch")
                   .otherwise(col("trip_type")))


# Convert trip distance from miles to km and round to nearest decimal place
parsed_green_df = parsed_green_df.withColumn("trip_distance", round(parsed_green_df["trip_distance"] * 1.60934, 2))

# Filter out rows with negative fare amount
parsed_green_df = parsed_green_df.filter(col("fare_amount") >= 0)

# Extract hour
parsed_green_df = parsed_green_df.withColumn("hour", hour(parsed_green_df["lpep_pickup_datetime"]))

# Extract day
parsed_green_df = parsed_green_df.withColumn("day", dayofmonth(parsed_green_df["lpep_pickup_datetime"]))

# Extract month
parsed_green_df = parsed_green_df.withColumn("month", month(parsed_green_df["lpep_pickup_datetime"]))

# Extract year
parsed_green_df = parsed_green_df.withColumn("year", year(parsed_green_df["lpep_pickup_datetime"]))

# Extract day name
parsed_green_df = parsed_green_df.withColumn("day_name", date_format(parsed_green_df["lpep_pickup_datetime"], "EEEE"))

# Fill any remaining missing values with 0
parsed_green_df = parsed_green_df.fillna(0)





#-----------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------


def write_to_mysql_yellow(batch_df, batch_id):
    batch_df.write.jdbc(mysql_url, "yellow_tab",mode="append" , properties=mysql_properties)

def write_to_mysql_green(batch_df, batch_id):
    batch_df.write.jdbc(mysql_url, "green_tab", mode="append", properties=mysql_properties)
    
    
    
# def write_to_mysql_fvh(batch_df, batch_id):
    
#     batch_df.write.jdbc(mysql_url, "forhire_vehicle_table",mode="append" ,properties=mysql_properties)

#-----------------------------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------------------------

yellow_stream = parsed_yellow_df.writeStream \
    .foreachBatch(write_to_mysql_yellow) \
    .outputMode("append") \
    .start()

green_stream = parsed_green_df.writeStream \
    .foreachBatch(write_to_mysql_green) \
    .outputMode("append") \
    .start()

# fvh_stream = parsed_fvh.writeStream \
#     .foreachBatch(write_to_mysql_fvh) \
#     .outputMode("append") \
#     .start()







# Wait for the streaming queries to finish
yellow_stream.awaitTermination()
green_stream.awaitTermination()
# fvh_stream.awaitTermination() 
    
    
    
    
    
    
    
