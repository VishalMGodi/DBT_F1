import warnings
import fastf1 as ff1
from fastf1 import plotting
from fastf1 import utils
from kafka import KafkaProducer
import json
from matplotlib import pyplot as plt
from matplotlib.pyplot import figure
import datetime
import numpy as np
import pandas as pd
import time
from pyspark.sql.functions import to_timestamp

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
    # time.sleep(2)

producer.send(
    topic='lap',
    value={
        "end": "end"
    }
)

print("End of data")
producer.flush()
producer.close()