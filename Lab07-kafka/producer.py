
from confluent_kafka import Producer, TopicPartition
import socket

# ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic testtopic
# ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic testtopic2
# ./kafka-topics.sh --describe --zookeeper localhost:2181 --topic testtopic
# ./kafka-topics.sh --list --zookeeper localhost:2181 --topic testtopic


class App():
    def __init__(self):
        self.topic = 'big-streams2'
        self.conf_producer = {
            'bootstrap.servers': 'localhost:9092',
            'client.id': socket.gethostname(),
            # 'enable.idempotence': True,
        }
        self.producer = Producer(self.conf_producer)

    def produce(self, data):
        #self.producer.produce(self.topic, data)
        self.producer.produce(self.topic, key=None, value=data)
        self.producer.flush()


app = App()
app.produce("utec2022-1")
