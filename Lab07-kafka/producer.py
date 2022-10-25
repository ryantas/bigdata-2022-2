from confluent_kafka import Producer, TopicPartition
import socket

class App():
    def __init__(self) -> None:
        self.topic = "test-bigdata"
        self.conf_producer = {
            'bootstrap.servers': 'localhost:9092, localhost:9093, localhost:9094',        
            'client.id': socket.gethostname(),
        }
        self.producer = Producer(self.conf_producer)

    
    def produce(self, data):
        self.producer.produce(self.topic, key=None, value=data)
        self.producer.flush()


app = App()
app.produce('data-from-produce python api')