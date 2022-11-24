# https://supergloo.com/spark-sql/
# https://jdbc.postgresql.org/download.html
# https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
# https://kb.objectrocket.com/postgresql/how-to-run-an-sql-file-in-postgres-846


# check version postgresqk 
# sudo -u postgres psql
# SELECT version();
# SHOW server_version;

# create initial database
sudo service postgresql initdb

# setup authentication
echo "host    sparksql    sparksql     127.0.0.1/32   trust" > /tmp/pg
sudo cat /var/lib/pgsql/data/pg_hba.conf >> /tmp/pg
sudo cp /var/lib/pgsql/data/pg_hba.conf /var/lib/pgsql/data/pg_hba.conf.DIST
sudo mv /tmp/pg /var/lib/pgsql/data/pg_hba.conf


# set permissions so postgres can read csv files
chmod 755 /home/serendipita/Documents/utec/bigdata/utec-bigdata-2022-1/data/csv/clicks/

# create postgres account for cloudera user
sudo -u postgres psql -c "CREATE USER sparksql"

# make cloudera account admin
sudo -u postgres psql -c "ALTER USER sparksql with superuser"

# create default db for cloudera account
sudo -u postgres createdb sparksql


# decompress csv files
gzip -d ../data/csv/clicks/*.csv.gz

# set permissions so postgres can read csv files
chmod 644 ../data/csv/clicks/*.csv

# create and load tables for hands on
psql -f setup/init-postgres.sql


# add postgres jdbc jar to spark classpath
echo "export SPARK_CLASSPATH=/usr/share/java/postgresql-jdbc-8.4.704.jar" >> ~/.bashrc

# set environment variables to load spark libs in jupyter
#echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"notebook\"" >> ~/.bashrc
#echo "export PYSPARK_DRIVER_PYTHON=jupyter"  >> ~/.bashrc

psql sparksql -h 127.0.0.1 -d sparksql -a -f init-postgres.sql
# ls -l ../data/csv/

# psql sparksql -h 127.0.0.1 -d sparksql -a -f init-postgres.sql

# df2 = sqlsc.read.format("jdbc") \
#   .option("url", "jdbc:postgresql://localhost/sparksql?user=sparksql") \
#   .option("dbtable", "adclicks") \
#   .load()