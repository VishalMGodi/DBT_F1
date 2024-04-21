# Kafka consumer subscribed to 3 topics ["lap", "car", "weather"], store in mysql database for each of these topics

from kafka import KafkaConsumer
import json
import mysql.connector
from mysql.connector import Error

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        print("Connecting...")
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        # print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")
    

connection = create_connection("localhost", "root", "", "f1db")

dbstore_consumer = KafkaConsumer('dbstore', value_deserializer=lambda m: json.loads(m.decode('ascii')))


lap_table = """
CREATE TABLE IF NOT EXISTS lap (
    Timestamp DATETIME,
    LapNumber INT,
    Driver VARCHAR(255),
    LapTime FLOAT,
    Stint INT,
    Sector1Time FLOAT,
    Sector2Time FLOAT,
    Sector3Time FLOAT,
    Compound VARCHAR(255),
    TyreLife INT,
    Position INT
);
"""

car_table = """
CREATE TABLE IF NOT EXISTS car (
    Timestamp DATETIME,
    LapNumber INT,
    Speed FLOAT,
    Throttle FLOAT,
    Brake INT,
    Gear INT,
    RPM INT,
    DRS INT
);
""" 

weather_table = """
CREATE TABLE IF NOT EXISTS weather (
    Timestamp DATETIME,
    LapNumber INT,
    AirTemp FLOAT,
    Humidity FLOAT,
    Pressure FLOAT,
    Rainfall INT,
    TrackTemp FLOAT,
    WindSpeed FLOAT,
    WindDirection FLOAT,
    Race VARCHAR(255)
);
"""

execute_query(connection, lap_table)
execute_query(connection, car_table)
execute_query(connection, weather_table)

# Read data from Kafka topics and store in MySQL database
def read_and_store_data():
    for message in dbstore_consumer:
        try:
            if("end" in message.value["car"].keys() or "end" in message.value["lap"].keys() or "end" in message.value["weather"].keys()):
                print("End of data")
                break
        except:
            pass
        query = f"""
        INSERT INTO lap (Timestamp, LapNumber, Driver, LapTime, Stint, Sector1Time, Sector2Time, Sector3Time, Compound, TyreLife, Position)
        VALUES ('{message.value["lap"]["Timestamp"]}', {message.value["lap"]["LapNumber"]}, '{message.value["lap"]["Driver"]}', {message.value["lap"]["LapTime"]}, {message.value["lap"]["Stint"]}, {message.value["lap"]["Sector1Time"]}, {message.value["lap"]["Sector2Time"]}, {message.value["lap"]["Sector3Time"]}, '{message.value["lap"]["Compound"]}', {message.value["lap"]["TyreLife"]}, {message.value["lap"]["Position"]});
        """
        execute_query(connection, query)
        print("Lap data stored")

        query = f"""
        INSERT INTO weather (Timestamp, LapNumber, AirTemp, Humidity, Pressure, Rainfall, TrackTemp, WindSpeed, WindDirection)
        VALUES ('{message.value["weather"]["Timestamp"]}', {message.value["weather"]["LapNumber"]}, {message.value["weather"]["AirTemp"]}, {message.value["weather"]["Humidity"]}, {message.value["weather"]["Pressure"]}, {message.value["weather"]["Rainfall"]}, {message.value["weather"]["TrackTemp"]}, {message.value["weather"]["WindSpeed"]}, {message.value["weather"]["WindDirection"]});
        """
        execute_query(connection, query)
        print("Weather data stored")

        list_of_car_data = message.value["car"]
        print("Car data storing...",end="")
        for car_data in list_of_car_data:
            query = f"""
            INSERT INTO car (Timestamp, LapNumber, Speed, Throttle, Brake, Gear, RPM, DRS)
            VALUES ('{car_data["Timestamp"]}', {car_data["LapNumber"]}, {car_data["Speed"]}, {car_data["Throttle"]}, {car_data["Brake"]}, {car_data["Gear"]}, {car_data["RPM"]}, {car_data["DRS"]});
            """
            execute_query(connection, query)
        print("Car data stored")
        print("----------------")

        
read_and_store_data()

dbstore_consumer.close()

connection.close()