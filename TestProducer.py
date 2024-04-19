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

def timeConverter(time):
    return time.total_seconds()
    # time = time.replace('0 days', '')
    # hours, minutes, seconds = map(float, time.split(':'))
    # return minutes*60 + seconds

warnings.simplefilter(action='ignore')

producer = KafkaProducer(value_serializer = lambda m: json.dumps(m).encode('ascii'))
ff1.Cache.enable_cache('cache')

race = ff1.get_session(2024, 1, 'R')
race.load()
laps = race.laps.pick_driver("VER").pick_quicklaps().reset_index()
    
for i in range(laps.shape[0]):
    lap = laps.iloc[i]
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

print("End of data")
producer.flush()
producer.close()