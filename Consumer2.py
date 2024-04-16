from kafka import KafkaConsumer
import json

weather_consumer = KafkaConsumer('weather', value_deserializer=lambda m: json.loads(m.decode('ascii')))

for message in weather_consumer:
    if("end" in message.value.keys()):
        print("End of data")
        break
    print("LapNumber: ",message.value["LapNumber"])
    print("AirTemp: ",message.value["AirTemp"])
    print("Humidity: ",message.value["Humidity"])
    print("Pressure: ",message.value["Pressure"])
    print("Rainfall: ",message.value["Rainfall"])
    print("TrackTemp: ",message.value["TrackTemp"])
    print("WindSpeed: ",message.value["WindSpeed"])
    print("WindDirection: ",message.value["WindDirection"])
    print("*"*100)

weather_consumer.close()