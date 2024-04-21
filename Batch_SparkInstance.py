from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import avg, sum, array_max, mode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

spark = SparkSession.builder \
    .appName("F1BatchProcessing") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .config("spark.jars", "file:///home/varun/Projects/DBT_F1/dependency/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46.jar") \
    .config("spark.executor.extraClassPath", "file:///home/varun/Projects/DBT_F1/dependency/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46.jar") \
    .config("spark.executor.extraLibrary", "file:///home/varun/Projects/DBT_F1/dependency/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46.jar") \
    .config("spark.driver.extraClassPath", "file:///home/varun/Projects/DBT_F1/dependency/mysql-connector-java-5.1.46/mysql-connector-java-5.1.46.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')


mysql_db_driver_class = "com.mysql.jdbc.Driver"
host_name = "localhost"
user_name = "root"
password = ""
port_no = "3306"
database_name = "f1db"



mysql_jdbc_url = f"jdbc:mysql://{host_name}:{port_no}/{database_name}?autoReconnect=true&useSSL=false"

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

lap_table_name = "lap"
db_lap_query = f"(SELECT * FROM {lap_table_name}) AS {lap_table_name}"
lap_df = spark.read.format("jdbc") \
    .option("url", mysql_jdbc_url) \
    .option("driver", mysql_db_driver_class) \
    .option("dbtable", db_lap_query) \
    .option("user", user_name) \
    .option("password", password) \
    .load()

car_table_name = "car"
db_car_query = f"(SELECT * FROM {car_table_name}) AS {car_table_name}"
car_df = spark.read.format("jdbc") \
    .option("url", mysql_jdbc_url) \
    .option("driver", mysql_db_driver_class) \
    .option("dbtable", db_car_query) \
    .option("user", user_name) \
    .option("password", password) \
    .load()

weather_table_name = "weather"
db_weather_query = f"(SELECT * FROM {weather_table_name}) AS {weather_table_name}"
weather_df = spark.read.format("jdbc") \
    .option("url", mysql_jdbc_url) \
    .option("driver", mysql_db_driver_class) \
    .option("dbtable", db_weather_query) \
    .option("user", user_name) \
    .option("password", password) \
    .load()

print("Lap Data")
lap_df.groupBy("Driver", "Stint", "Compound").agg({"LapTime": "avg", "LapTime": "max"}).show()

print("Car Data")
car_df.groupBy("LapNumber").agg({"Throttle":"avg", "Speed":"avg", "RPM":"avg", "Gear":"mode", "Speed":"max"}).show()

print("Weather Data")
weather_df.groupBy("Race").agg({"AirTemp":"avg", "Humidity":"avg", "TrackTemp":"avg", "WindSpeed":"avg"}).show()

while(True):
    pass

spark.stop()