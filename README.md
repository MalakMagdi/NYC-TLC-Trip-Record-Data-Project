# NYC-TLC-Trip-Record-Data-Project

## Introduction

The TLC Trip Record Data Project aims to gather data from the NYC-TLC (Taxi and Limousine Commission) Trip Record data source for the years 2019 to 2023. The data sources include Yellow Taxi, Green Taxi, and For-Hire Vehicle trip records. The data collection is simulated using a Python script, and the records are pushed to Kafka for further processing. The project utilizes various technologies like Docker, Kafka, Spark, Python, MySQL, and Power BI for data transformation, staging, ETL (Extract, Transform, Load), and analysis.

## Data Collection

Data collection is simulated through a Python script that generates random trip records for each trip type (Yellow Taxi, Green Taxi, and For-Hire Vehicle) at 10-second intervals. The number of records generated per interval varies randomly between 100 and 500 records.

## Data Processing

The simulated trip records are pushed to a Kafka message broker, ensuring a reliable and scalable data ingestion process. Kafka provides the necessary infrastructure to handle the incoming data streams.

## Staging Layer

The data is then processed using PySpark to perform transformation tasks specific to each car type. The staging layer is created using MySQL, where the cleaned and slightly transformed data is stored temporarily.

## ETL Process

After data processing and staging, another ETL step is performed using Python to transfer the prepared data to the data warehouse. The data warehouse, built using MySQL, serves as a centralized repository for the analyzed data.

## Data Warehouse

The data warehouse stores the cleaned and transformed data in a structured manner, allowing for efficient querying and analysis. The data warehouse facilitates easier integration with various business intelligence tools.
![Model](https://github.com/MalakMagdi/TLC-Trip-Record-Data-Project/assets/110945022/07615f2c-7382-4d34-86c6-921625c780cb)


## Time Series Analysis

To derive meaningful insights from the data, a time series analysis is conducted using Power BI. The analysis results are visualized through interactive dashboards, enabling users to explore trends and patterns effectively.
![Dashboard1](https://github.com/MalakMagdi/TLC-Trip-Record-Data-Project/assets/110945022/105d1669-2faa-403b-8dbb-1e0bed6a719a)
![Dashboard2](https://github.com/MalakMagdi/TLC-Trip-Record-Data-Project/assets/110945022/c0e0b343-37fc-4435-aa10-a26c968163ca)



## Technologies Used

- Docker
- Apache Kafka
- Apache Spark
- Python
- MySQL
- Power BI

