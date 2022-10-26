
from confluent_kafka import Consumer, TopicPartition
import redis

REDIS_HOST = '0.0.0.0'
REDIS_PORT = '6379'
redis = redis.Redis(host=self.redis_host,
                                 port=self.redis_port, decode_responses=True)


'''
    Topic: Weather
    partitions: 3
    fr: 2

    p0: d00,d01,d02(commit = True)
    p1: d10,d11(commit = True) 
    p2: d20,d21,d22(commit=True),d23(commit=True)

    d = poll(1seg) -> d11, d23, d22

'''

class DataCapture():
    def __init__(self) -> None:
        self.conf = {
            'bootstrap.servers': 'localhost:9092, localhost:9093, localhost:9094',
            'group.id': 'test2',            
            'enable.auto.commit': 'false',
            'auto.offset.reset': 'earliest',
            'max.poll.interval.ms': '500000',
            'session.timeout.ms': '120000',
            'request.timeout.ms': '120000'
        }

    def consume(self, topic='test-bigdata'):
        self.consumer = Consumer(self.conf)
        self.topic = topic
        self.consumer.subscribe([self.topic])

        try:
            while True:
                # msg = self.consumer.poll(1.0) # consume(100, 1.0)
                msgs = self.consumer.consume(100, 1.0)                
                if msgs is None:
                    continue
                for msg in msgs:
                    user = msg.value()
                    partition = msg.partition()
                    offset = msg.offset()
                    if user is not  None:
                        # process it
                        print(user)
                    self.consumer.commit(offsets=[TopicPartition(topic = self.topic, partition=partition, offset=offset+1)], asynchronous = False)

                print("==============================")

        except KeyboardInterrupt:
            print("interrupted error ")
        
        self.consumer.close()


capture = DataCapture()
capture.consume('test-bigdata')