gnome-terminal --tab -- bin/zookeeper-server-start.sh config/zookeeper.properties
gnome-terminal --tab -- bin/kafka-server-start.sh config/server.properties
gnome-terminal --tab -- bin/kafka-server-start.sh config/server-1.properties
gnome-terminal --tab -- bin/kafka-server-start.sh config/server-2.properties

# gnome-terminal --tab -- bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic test

# bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic test
# bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test
# bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
# bin/kafka-console-producer.sh --zookeeper localhost:2181  --topic test

# works!
# bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
# bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test
