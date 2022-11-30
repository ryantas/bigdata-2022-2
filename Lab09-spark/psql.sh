
# https://www.folkstalk.com/2022/09/postgres-list-users-and-roles-with-code-examples.html

# sudo -u postgres psql # `or`  psql -U postgres -W
# SELECT version();

# copy jdbc driver.jar in spark/jars in that's it

# >> \d
# >> \dt
# >> \list or \l

# >> du  # users and roles

# \list or \l: list all databases
# \c <db name>: connect to a certain database
# \dt: list all tables in the current database using your search_path
# \dt *.: list all tables in the current database regardless your search_path


# export SPARK_CLASSPATH=/usr/share/java/postgresql-jdbc-8.4.704.jar
