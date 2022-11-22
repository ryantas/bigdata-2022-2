# if you have already install spark before
unset SPARK_HOME
# add these lines on .bashrc
export SPARK_HOME=/home/{user_pc}/Documents/utec/bigdata/spark-3.2.1-bin-hadoop3.2
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PATH=$PATH:$JAVA_HOME/jre/bin

# use python >= 3.9
# conda activate bigdata-2022-2