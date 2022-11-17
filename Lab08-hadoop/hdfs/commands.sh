
# docker-compose -f docker-compose.yml up
# sudo docker cp ../../datasets/raw/words.txt namenode:/workspace/
# sudo docker cp ./wordCount.py namenode:/workspace/

# hdfs dfs -put /workspace/words.txt /input
# hdfs dfs -ls /workspace/

# 
# chmod +x wordCount.py



# track datanode
# http://localhost:8042/node