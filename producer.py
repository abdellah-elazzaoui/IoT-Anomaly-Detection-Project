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
        devices = ["DHT11_A", "DHT11_B", "DHT11_C", "DHT11_D"]
        device_id = random.choice(devices)
        
        return {
            "Device_ID": device_id,
            "timestamp": datetime.now().isoformat(),
            "Temperature": round(random.uniform(-1.5, 1.5), 6),
            "Humidity": round(random.uniform(-1.5, 1.5), 6),
            "Battery_Level": round(random.uniform(-1.5, 1.5), 6)   
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
