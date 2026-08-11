from confluent_kafka.admin import AdminClient , NewTopic

class Admin:
    def __init__(self,bootstrapserver):
        self.bootstrapserver = bootstrapserver
        self.admin = AdminClient({"bootstrap.servers":self.bootstrapserver})

    def topic_exist(self,topic:str):
        all_topics = self.admin.list_topics()
        return topic in all_topics.topics
    def create_topic(self,topic:str,partition:int):
        if not self.topic_exist(topic):
            new_topic = NewTopic(topic=topic , num_partitions = partition , replication_factor=3)
            self.admin.create_topics([new_topic])
            print(f"Topic {topic} has been created !!!")
            return
        print("Topic Already Exists")


if __name__ == "__main__":
    bootstrapserver = "localhost:9092,localhost:9093,localhost:9094"
    admin = Admin(bootstrapserver=bootstrapserver)
    try:
        admin.create_topic("test",8)
    except Exception as e:
        print(f"ERROR - {str(e)}")    