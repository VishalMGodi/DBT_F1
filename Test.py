from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, from_json, explode, window, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

spark = SparkSession.builder \
    .appName("F1LapTimeProcessor") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.jars", "file:///home/varun/Projects/DBT_F1/dependencies/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46.jar") \
    .config("spark.executor.extraClassPath", "file:///home/varun/Projects/DBT_F1/dependencies/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46.jar") \
    .config("spark.executor.extraLibrary", "file:///home/varun/Projects/DBT_F1/dependencies/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46.jar") \
    .config("spark.driver.extraClassPath", "file:///home/varun/Projects/DBT_F1/dependencies/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

mysql_db_driver_class = "com.mysql.jdbc.Driver"
mysql_db_url = "jdbc:mysql://localhost:3306/f1db"
table_name = "lap_times"
host_name = "localhost"
user_name = "root"
password = ""
port_no = "3306"
database_name = "f1db"

mysql_select_query = f"(SELECT * FROM {table_name}) AS {table_name}"
# print("mysql_select_query: ")
# print(mysql_select_query)

mysql_jdbc_url = f"jdbc:mysql://{host_name}:{port_no}/{database_name}?autoReconnect=true&useSSL=false"
# print("mysql_jdbc_url: ")
# print(mysql_jdbc_url)

laps_df = spark.read.format("jdbc") \
    .option("url", mysql_jdbc_url) \
    .option("driver", mysql_db_driver_class) \
    .option("dbtable", mysql_select_query) \
    .option("user", user_name) \
    .option("password", password) \
    .load()

laps_df.createOrReplaceTempView("lap_times")

laps_query = spark.sql("SELECT * FROM lap_times")
laps_query.show(10,False)

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

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "lap") \
    .load()\
    # .filter(col("LapTime").isNotNull())

df_processed = df.select(F.from_json(F.col("value").cast("string"), schema).alias("json_data")) \
                  .select("json_data.*") \
                  .filter(col("LapTime").isNotNull())

# Store the processed data in a temporary view
df_processed.createOrReplaceTempView("lap_times_processed")

processed_select_query = spark.sql("SELECT * FROM lap_times_processed")

streaming_query = processed_select_query.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

windowed_df = df_processed.withWatermark("Timestamp", "0 seconds") \
                          .groupBy(col("Driver"), col("Stint"), col("Compound"), window(col("Timestamp"), "600 seconds")) \
                          .agg(avg("LapTime").alias("AverageLapTime"), sum("LapTime").alias("TotalTime"))

query = windowed_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

# query.awaitTermination()

spark.stop()
