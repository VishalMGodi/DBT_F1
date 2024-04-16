from kafka import KafkaConsumer
import json

lap_consumer = KafkaConsumer('lap', value_deserializer=lambda m: json.loads(m.decode('ascii')))
for message in lap_consumer:
    if("end" in message.value.keys()):
        print("End of data")
        break
    print("Driver: ",message.value["Driver"])
    print("Position: ",message.value["Position"])
    print("LapTime: ",message.value["LapTime"])
    print("LapNumber: ",message.value["LapNumber"])
    print("Compound: ",message.value["Compound"])
    print("*"*100)

lap_consumer.close()
