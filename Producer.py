import warnings
import fastf1 as ff1
from kafka import KafkaProducer
import json
import datetime
import time

warnings.simplefilter(action='ignore')

producer = KafkaProducer(value_serializer = lambda m: json.dumps(m).encode('ascii'))
ff1.Cache.enable_cache('cache')

race = ff1.get_session(2024, 1, 'R')
race.load()
laps = race.laps.pick_driver("VER").pick_quicklaps().reset_index()


def convert_to_hours_minutes_seconds(input_string):

    return input_string.replace("0 days ", "")

for i in range(1, laps.shape[0]):
    lap = laps.iloc[i]
    weather = lap.get_weather_data()
    car = lap.get_car_data()
    for j in range(car.shape[0]):
        point = car.iloc[j]
        producer.send(
            topic='car',
            value={
                'Timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'LapNumber': int(lap['LapNumber']),
                'Speed': float(point['Speed']),
                'Throttle': float(point['Throttle']),
                'Brake': int(point['Brake']),
                'Gear': int(point['nGear']),
                'RPM': int(point['RPM']),
                'DRS': int(point['DRS'])
            }
        )
    print(f"Car data for lap {i+1} sent")
    producer.send(
        topic='lap',
        value={
            'Timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'LapNumber': int(lap['LapNumber']),
            'Driver': lap['Driver'],
            'LapTime': float(lap['LapTime'].total_seconds()),
            'Stint': int(lap['Stint']),
            'Sector1Time': float(lap['Sector1Time'].total_seconds()),
            'Sector2Time': float(lap['Sector2Time'].total_seconds()),
            'Sector3Time': float(lap['Sector3Time'].total_seconds()),
            'Compound': lap['Compound'],
            'TyreLife': int(lap['TyreLife']),
            'Position': int(lap['Position'])
        }
    )
    print(f"Lap {i+1} sent : {float(lap['LapTime'].total_seconds())}")
    producer.send(
        topic = "weather",
        value = {
            'Timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'LapNumber': int(lap['LapNumber']),
            'AirTemp': float(weather['AirTemp']),
            'Humidity': float(weather['Humidity']),
            'Pressure': float(weather['Pressure']),
            'Rainfall': int(weather['Rainfall']),
            'TrackTemp': float(weather['TrackTemp']),
            'WindSpeed': float(weather['WindSpeed']),
            'WindDirection': float(weather['WindDirection']),
            'Race': "Bahrein GP",
        }
    )
    print(f"Weather data for lap {i+1} sent")
    dbstore_value = {}
    dbstore_value['lap'] = {
                'LapNumber': int(lap['LapNumber']),
                'Driver': lap['Driver'],
                'LapTime': float(lap['LapTime'].total_seconds()),
                'Stint': int(lap['Stint']),
                'Sector1Time': float(lap['Sector1Time'].total_seconds()),
                'Sector2Time': float(lap['Sector2Time'].total_seconds()),
                'Sector3Time': float(lap['Sector3Time'].total_seconds()),
                'Compound': lap['Compound'],
                'TyreLife': int(lap['TyreLife']),
                'Position': int(lap['Position'])
            }
    dbstore_value['weather'] = {
                'Timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'LapNumber': int(lap['LapNumber']),
                'AirTemp': float(weather['AirTemp']),
                'Humidity': float(weather['Humidity']),
                'Pressure': float(weather['Pressure']),
                'Rainfall': int(weather['Rainfall']),
                'TrackTemp': float(weather['TrackTemp']),
                'WindSpeed': float(weather['WindSpeed']),
                'WindDirection': float(weather['WindDirection']),
            }
    dbstore_value['car'] = []
    for j in range(car.shape[0]):
        point = car.iloc[j]
        dbstore_value['car'].append({
                'Timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'LapNumber': int(lap['LapNumber']),
                'Speed': float(point['Speed']),
                'Throttle': float(point['Throttle']),
                'Brake': int(point['Brake']),
                'Gear': int(point['nGear']),
                'RPM': int(point['RPM']),
                'DRS': int(point['DRS'])
            })
    
    producer.send(
        topic = "dbstore",
        value = dbstore_value
    )
    print(f"Data for lap {i+1} sent")
    print("-------------")
    
    time.sleep(10)


producer.send(
    topic='lap',
    value={
        "end": "end"
    }
)

producer.send(
    topic='weather',
    value={
        "end": "end"
    }
)

producer.send(
    topic='car',
    value={
        "end": "end"
    }
)

print("End of data")
producer.flush()
producer.close()