from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, from_json, explode, window, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
# from pyspark.kafka import KafkaUtils

# Create a SparkSession
spark = SparkSession.builder \
    .appName("F1LapTimeProcessor") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

# Kafka consumer configuration (replace with your details)
bootstrap_servers = "localhost:9092"
subscribe_topic = "lap"
kafka_params = {
    "bootstrap.servers": bootstrap_servers,
    "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
    # "group.id": "f1-lap-consumer",
    "enable.auto.commit": False  # Manually commit offsets for better control
}

# Define a schema for the JSON data
schema = StructType([
    StructField("LapNumber", IntegerType(), True),
    StructField("Driver", StringType(), True),
    StructField("LapTime", FloatType(), True),
    StructField("Stint", IntegerType(), True),
    StructField("Sector1Time", FloatType(), True),
    StructField("Sector2Time", FloatType(), True),
    StructField("Sector3Time", FloatType(), True),
    StructField("Compound", StringType(), True),
    StructField("TyreLife", IntegerType(), True),
    StructField("Position", IntegerType(), True)
    # col("LapNumber", "integer"),
    # col("Driver", "string"),
    # col("LapTime", "float"),
    # col("Stint", "integer"),
    # col("Sector1Time", "float"),
    # col("Sector2Time", "float"),
    # col("Sector3Time", "float"),
    # col("Compound", "string"),
    # col("TyreLife", "integer"),
    # col("Position", "integer")
])

# Create a Kafka streaming DataFrame
# df = spark \
#     .readStream \
#     .format("kafka") \
#     .options(**kafka_params) \
#     .option("subscribe", subscribe_topic) \
#     .load()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "lap") \
    .load()\
    .select(F.from_json(F.col("value").cast("string"), schema).alias("json_data"))\
    .select("json_data.*")


write = df\
    .writeStream\
    .outputMode("append")\
    .format("console")\
    .option("truncate", False)\
    .start()

write.awaitTermination()
# Parse JSON payload and select relevant columns
# df_processed = df.selectExpr("CAST(value AS STRING)", "cast(value AS BINARY) AS raw_data") \
#                  .withColumn("data", from_json(col("raw_data"), schema)) \
#                  .select("data.*") \
#                  .filter(col("LapNumber").isNotNull())  # Filter out the "end" signal

# # Calculate average lap times and total time spent per stint
# windowed_df = df_processed.withWatermark("LapNumber", "0 seconds") \
#                           .groupBy(col("Driver"), window(col("LapNumber"), "2 laps")) \
#                           .agg(avg("LapTime").alias("AverageLapTime"), sum("LapTime").alias("TotalTime"))

# # Start the streaming query and print results
# query = windowed_df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()

# query.awaitTermination()


# ... your streaming DataFrame transformations ...

# print("\n\n\n\nSpark to SQL to Kafka successful\n\n\n\n\n")

# query = df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .start()  # Start the streaming query

# df.printSchema()
# df.show()

# query.awaitTermination()  # Wait for the query to finish (optional)


# Stop the SparkSession
spark.stop()
