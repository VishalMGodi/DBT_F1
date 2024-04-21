from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, from_json, explode, window, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

spark = SparkSession.builder \
    .appName("F1WeatherProcessor") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')


weather_schema = StructType([
    StructField("Timestamp", TimestampType(), True),
    StructField("LapNumber", IntegerType(), True),
    StructField("AirTemp", FloatType(), True),
    StructField("Humidity", FloatType(), True),
    StructField("Pressure", FloatType(), True),
    StructField("Rainfall", IntegerType(), True),
    StructField("TrackTemp", FloatType(), True),
    StructField("WindSpeed", FloatType(), True),
    StructField("WindDirection", FloatType(), True),
    StructField("Race", StringType(), True),
])

weather_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather") \
    .load()\

parsed_weather_df = weather_df.select(F.from_json(F.col("value").cast("string"), weather_schema).alias("json_data")) \
                  .select("json_data.*") \
                  .filter(col("LapNumber").isNotNull())


windowed_df = parsed_weather_df.withWatermark("Timestamp", "0 seconds")\
                            .groupBy(col("Race"), window(col("Timestamp"), "50 seconds", "10 seconds"))\
                            .agg(avg("AirTemp"),avg("Humidity"),avg("TrackTemp"), avg("WindSpeed"))

aggregated_df_write = windowed_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

aggregated_df_write.awaitTermination()

spark.stop()
