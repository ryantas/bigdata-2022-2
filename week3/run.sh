# create database transaction
docker-compose run -d --rm --name myneo -p 7687:7687 neo4j
docker exec -it myneo bash
cypher-shell

# or -> 
# docker run --rm --name neo4j --env NEO4J_AUTH=neo4j/pass -p 7474:7474 -p 7687:7687 neo4j:latest
# docker exec -ti neo4j cypher-shell -u neo4j -p pass

docker-compose run -d --rm --name mycassandra -p 9042:9042 cassandra
docker exec -it mycassandra /bin/sh
cqlsh -u cassandra -p cassandra
show version

docker stop mycassandra
docker rm mycassandra

createdb mypsql
psql mypsql < config.sql 

# https://cassandra.apache.org/_/quickstart.html

# https://splunktool.com/how-to-pass-along-username-and-password-to-cassandra-in-python

# https://docs.datastax.com/en/developer/python-driver/3.23/security/

# https://citizix.com/how-to-run-cassandra-4-with-docker-and-docker-compose/

# https://www.cdata.com/kb/tech/cassandra-python-petl.rst



# comparison between POSTGRESQL, CASSANDRA AND MONGODB
# https://medium.com/yugabyte/data-modeling-basics-postgresql-vs-cassandra-vs-mongodb-208a5fa2dd22

# jupyterhub
# ports 8000 or 5000 available
sudo chmod -R 775 /usr/local/lib/node_modules/


# https://github.com/jupyterhub/configurable-http-proxy