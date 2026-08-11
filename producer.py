from confluent_kafka import Producer
from admin import Admin
from uuid import uuid4
import random
from datetime import datetime
import json
import time

def delivery_report(err,msg):
    if err is not None:
        print(f"Filed to Delivery Message : {msg.key()}")
        return
    print(f"The Message Successfully Delivred Key : {msg.key()} , Topic : {msg.topic()} , Partition : {msg.partition()} , offset : {msg.offset()}")


class SensorProducer:
    
    def __init__(self,bootstrapserver,topic,message_size,compression_type,batch_size=None,waiting_time=None):
        self.bootstrapserver = bootstrapserver
        self.topic = topic
        config = {"bootstrap.servers":self.bootstrapserver}
        if message_size:
            config['message.max.bytes'] = message_size
        if compression_type:
            config["compression.type"] = compression_type
        if batch_size :
            config['batch.size'] = batch_size
        if waiting_time :
            config["linger.ms"] = waiting_time
        config['partitioner'] = "random"
        self.producer = Producer(config)
        self.count = 0 

    def create_message(self):
        # Normal ranges
        temperature = round(random.uniform(20, 35), 2)  # 20-35°C
        humidity = round(random.uniform(30, 70), 1)     # 30-70%
        pressure = round(random.uniform(980, 1020), 1)  # 980-1020 hPa
        vibration = round(random.uniform(0.1, 1.5), 2)  # 0.1-1.5 m/s²
        
        return {
            "timestamp": datetime.now().isoformat(),
            "device_id": f"DEV-{random.randint(1, 10):03d}",
            "temperature": temperature,
            "humidity": humidity,
            "pressure": pressure,
            "vibration": vibration,
            "status": "normal"
        }                    

    def send_message(self):
        try:
            message = json.dumps(self.create_message())
            if message:
                self.producer.produce(
                    topic = self.topic,
                    value=message,
                    key = str(self.count),
                    headers = {"correlation_id":str(uuid4())},
                    on_delivery = delivery_report
                )
                self.commit()
        except Exception as e:
            print(f"ERROR : {str(e)}")

    def commit(self):
        self.producer.flush()



if __name__ == "__main__":
    bootstrapserver = "localhost:9092,localhost:9093,localhost:9094"
    topic = "iot_data"
    admin = Admin(bootstrapserver)
    admin.create_topic(topic,8)
    producer = SensorProducer(bootstrapserver,topic,4*1024,"snappy")
    producer.send_message()
