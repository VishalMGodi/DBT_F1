import warnings
import fastf1 as ff1
from fastf1 import plotting
from fastf1 import utils
from kafka import KafkaProducer
import json
from matplotlib import pyplot as plt
from matplotlib.pyplot import figure
from datetime import timedelta

import numpy as np
import pandas as pd

warnings.simplefilter(action='ignore')

producer = KafkaProducer(value_serializer = lambda m: json.dumps(m).encode('ascii'))
ff1.Cache.enable_cache('cache')

race = ff1.get_session(2024, 1, 'R')
race.load()
laps = race.laps.pick_driver("VER").pick_quicklaps().reset_index()
    
for i in range(laps.shape[0]):
    lap = laps.iloc[i]
    telemetry = lap.get_car_data()
    weather = lap.get_weather_data()
    producer.send(
        topic='weather',
        value={
            'LapNumber': int(lap['LapNumber']),
            'AirTemp': float(weather['AirTemp']),
            'Humidity': float(weather['Humidity']),
            'Pressure': float(weather['Pressure']),
            'Rainfall': int(weather['Rainfall']),
            'TrackTemp': float(weather['TrackTemp']),
            'WindSpeed': float(weather['WindSpeed']),
            'WindDirection': float(weather['WindDirection'])
        }
    )
    for j in range(telemetry.shape[0]):
        point = telemetry.iloc[j]
        producer.send(
            topic='telemetry',
            value={
                'LapNumber': int(lap['LapNumber']),
                'Time': float(point['Time'].total_seconds()),
                'Speed': float(point['Speed']),
                'Throttle': float(point['Throttle']),
                'Brake': int(point['Brake']),
                'Gear': int(point['nGear']),
                'RPM': int(point['RPM']),
                'DRS': int(point['DRS'])
            }
        )
    producer.send(
        topic='lap',
        value={
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
    topic='telemetry',
    value={
        "end": "end"
    }
)

print("End of data")
producer.flush()
producer.close()