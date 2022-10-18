from confluent_kafka import Consumer, TopicPartition

# ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic testtopic
# ./kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic testtopic2
# ./kafka-topics.sh --describe --zookeeper localhost:2181 --topic testtopic
# ./kafka-topics.sh --list --zookeeper localhost:2181 --topic testtopic


class DataCapture():
    def __init__(self):
        self.conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'test',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'false',
            'max.poll.interval.ms': 500000,
            'session.timeout.ms': 120000,
            'request.timeout.ms': 120000,
        }

    def consume(self, topic='big-streams2'):
        self.consumer = Consumer(self.conf)
        self.topic = topic
        self.consumer.subscribe([self.topic])
        try:
            while True:
                msg = self.consumer.poll(1.0)  # .consume(10)
                if msg is None:
                    continue
                user = msg.value()
                partition = msg.partition()
                offset = msg.offset()

                if user is not None:
                    print(f"partition: {partition}")
                    print(f"offset: {offset}")
                    print(user)
                    # print(f'User name: {user.name}, '
                    #       f'favorite number:{user.favorite_number}, '
                    #       f'favorite color:{user.favorite_color}, '
                    #       f'twitter handle:{user.twitter_handle}')
                # self.consumer.commit([TopicPartition(partition, offset)])
                self.consumer.commit(
                    offsets=[TopicPartition(topic=self.topic,
                                            partition=partition,
                                            offset=offset+1)], asynchronous=False)
        except KeyboardInterrupt:

            print("error")
        print('closing the consumer')
        self.consumer.close()


capture = DataCapture()
capture.consume('bigdata-streams')
