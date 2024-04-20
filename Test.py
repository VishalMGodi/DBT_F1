from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, from_json, explode, window, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Create a SparkSession
spark = SparkSession.builder \
    .appName("F1LapTimeProcessor") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel('OFF')

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
    StructField("Timestamp", TimestampType(), True),
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
    # .filter(col("LapTime").isNotNull())

df_processed = df.select(F.from_json(F.col("value").cast("string"), schema).alias("json_data")) \
                  .select("json_data.*") \
                  .filter(col("LapTime").isNotNull())  # Filter out the "end" signal

# Calculate average lap times and total time spent per stint
windowed_df = df_processed.withWatermark("Timestamp", "0 seconds") \
                          .groupBy(col("Driver"), col("Stint"), col("Compound"), window(col("Timestamp"), "600 seconds")) \
                          .agg(avg("LapTime").alias("AverageLapTime"), sum("LapTime").alias("TotalTime"))

# Start the streaming query and print results
query = windowed_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

# Stop the SparkSession
spark.stop()
