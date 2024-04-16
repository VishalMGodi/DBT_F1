from kafka import KafkaConsumer
import json

telemetry_consumer = KafkaConsumer('telemetry', value_deserializer=lambda m: json.loads(m.decode('ascii')))

for message in telemetry_consumer:
    if("end" in message.value.keys()):
        print("End of data")
        break
    print("LapNumber: ",message.value["LapNumber"])
    print("Time: ",message.value["Time"])
    print("Speed: ",message.value["Speed"])
    print("Throttle: ",message.value["Throttle"])
    print("Brake: ",message.value["Brake"])
    print("*"*100)