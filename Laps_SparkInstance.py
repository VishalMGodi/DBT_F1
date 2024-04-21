from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, from_json, explode, window, avg, sum, array_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

spark = SparkSession.builder \
    .appName("F1LapTimeProcessor") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')


lap_schema = StructType([
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

lap_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "lap") \
    .load()\

parsed_lap_df = lap_df.select(F.from_json(F.col("value").cast("string"), lap_schema).alias("json_data")) \
                  .select("json_data.*") \
                  .filter(col("LapTime").isNotNull())


windowed_df = parsed_lap_df.withWatermark("Timestamp", "0 seconds") \
                          .groupBy(col("Driver"), col("Stint"), col("Compound"), window(col("Timestamp"), "65 seconds")) \
                          .agg(avg("LapTime").alias("Average Lap Time"), array_max("LapTime").alias("Best Lap Time"))

aggregated_df_write = windowed_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

aggregated_df_write.awaitTermination()

spark.stop()
