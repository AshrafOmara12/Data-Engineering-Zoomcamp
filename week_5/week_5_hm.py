import pandas as pd
from pyspark.sql import types
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from prefect import task, flow

@task()
def create_spark_cluster():
    """ create a local spark cluster """
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()
    return spark

@task()
def define_schema(schema_name: str):
    """ define the schema of fhvhv data """
    if schema_name == "fhvhv":
        df_fhvhv_schema = types.StructType([
        types.StructField("dispatching_base_num", types.StringType(), True),
        types.StructField("pickup_datetime", types.TimestampType(), True),
        types.StructField("dropoff_datetime", types.TimestampType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("SR_Flag", types.StringType(), True),
        types.StructField("Affiliated_base_number", types.StringType(), True) ])
        return df_fhvhv_schema
    elif schema_name == "zones":
        df_zones_schema = types.StructType([
        types.StructField("LocationID", types.IntegerType(), True),
        types.StructField("Borough", types.StringType(), True),
        types.StructField("Zone", types.StringType(), True),
        types.StructField("service_zone", types.StringType(), True)])
        return df_zones_schema

@flow(log_prints=True)
def read_files(schema_: str):
    """ reading fhvhv data saved as csv """
    df_fhvhv = create_spark_cluster().read \
                        .option("header",True) \
                        .schema(define_schema(schema_)) \
                        .csv("fhvhv_tripdata_2021-06.csv.gz")
    print(df_fhvhv.show())
    df_zones = create_spark_cluster().read \
                .option("header", True) \
                .schema(define_schema(schema_)) \
                .csv('zones')
    print(df_zones.show())

@flow()
def batch_processing(schema_: str):
    create_spark_cluster()
    define_schema(schema_)
    read_files(schema_)
    
if __name__ == "__main__":
    for schema_ in ['fhvhv', 'zones']:
        batch_processing(schema_)
exit()
#


# 


#writing fhvhv data as parquet files and repartition to 12 files with same size

df_fhvhv \
.repartition(12) \
.write.parquet('data', mode= 'overwrite')

#reading the parquet files to test the output
df_fhvhv_parquet = spark.read.parquet('data')

# create a table using spark to start working on it as sql queries
df_fhvhv_parquet.createOrReplaceTempView('df_fhvhv_parquet')

#Question3: How many taxi trips were there on June 15?
trips_on_15_june = spark.sql("""
SELECT 
    COUNT(1)
FROM
    df_fhvhv_parquet
WHERE pickup_datetime 
    BETWEEN "2021-06-15 00:00:00" AND "2021-06-15 23:59:59"
""")
# Questiton3 Answer
print(trips_on_15_june.show())

#Question3 solution using pyspark dataframe filter function 
df_fhvhv_parquet \
.filter(F.col("pickup_datetime").between("2021-06-15 00:00:00", "2021-06-15 23:59:59")) \
.count()

# Question4: How long was the longest trip in Hours?
longest_trip = spark.sql("""
SELECT 
MAX(TIMESTAMPDIFF(SECOND, pickup_datetime, dropoff_datetime))/3600 AS difference
FROM
    df_fhvhv_parquet
""")

#Question4: Answer
print(longest_trip.show())

#zones dataset schema

#reading zones dataset

#create a table from zones df_zones
df_zones.createOrReplaceTempView("df_zones")

# Question6: Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?
most_frequent_zone = spark.sql(
"""
SELECT
       df_zones.Zone, COUNT(PULocationID)
FROM 
    df_fhvhv_parquet
JOIN 
    df_zones
ON 
    df_fhvhv_parquet.PULocationID = df_zones.LocationID
    
GROUP BY
    1
ORDER BY
    2 DESC
"""

)
# Question6: Answer
print(most_frequent_zone.show())