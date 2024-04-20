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

tmpList = []

def convert_to_hours_minutes_seconds(input_string):

    return input_string.replace("0 days ", "")

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
    tmpList.append(float(lap['LapTime'].total_seconds()))
    print(f"Lap {i+1} sent : {float(lap['LapTime'].total_seconds())}")
    time.sleep(10)


for i in range(0, len(tmpList)):
    avgCal = tmpList[i]
    print("-----",i, avgCal)
    for j in range(i+1, len(tmpList)):
        avgCal = (avgCal*(j-i) + tmpList[j]) / (j-i+1)
        print(avgCal)
    print("-----")
    break


producer.send(
    topic='lap',
    value={
        "end": "end"
    }
)

print("End of data")
producer.flush()
producer.close()