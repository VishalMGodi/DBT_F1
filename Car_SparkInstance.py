from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, avg, mode, array_max
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

spark = SparkSession.builder \
    .appName("F1CarProcessor") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')


car_schema = StructType([
    StructField("Timestamp", TimestampType(), True),
    StructField("LapNumber", IntegerType(), True),
    StructField("Speed", FloatType(), True),
    StructField("Throttle", FloatType(), True),
    StructField("Brake", IntegerType(), True),
    StructField("Gear", IntegerType(), True),
    StructField("RPM", IntegerType(), True),
    StructField("DRS", IntegerType(), True),
])

car_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "car") \
    .load()\

parsed_car_df = car_df.select(F.from_json(F.col("value").cast("string"), car_schema).alias("json_data")) \
                  .select("json_data.*") \
                  .filter(col("LapNumber").isNotNull())


windowed_df = parsed_car_df.withWatermark("Timestamp", "0 seconds") \
                          .groupBy(col("LapNumber")) \
                          .agg(avg("Throttle"), avg("Speed"),avg("RPM"), mode("Gear"), array_max("Speed"))

aggregated_df_write = windowed_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

aggregated_df_write.awaitTermination()

spark.stop()
