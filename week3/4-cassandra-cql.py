import logging

from cassandra import AlreadyExists
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.cqlengine import columns, connection, management, models, query
from cassandra.cqlengine.connection import log as cql_logger


def set_cql_logger():
    """activate cql engine logger
    """
    cql_logger.setLevel(logging.DEBUG)
    _formatter = logging.Formatter('%(message)s')
    _handler = logging.StreamHandler()
    _handler.setFormatter(_formatter)
    cql_logger.addHandler(_handler)


class City(models.Model):
    """date model
    """
    country = columns.Text(primary_key=True)
    city = columns.Text(primary_key=True)
    population = columns.Integer()
    gdp = columns.Integer()


def sync_db():
    """sync database
    """
    auth = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['127.0.0.1'], auth_provider=auth)    
    db_session = cluster.connect()
    keyspace = 'testing'
    try:
        db_session.execute(
            'CREATE KEYSPACE %s WITH replication = '
            "{'class': 'SimpleStrategy', 'replication_factor': '1'} "
            'AND durable_writes = true;' % keyspace)
    except AlreadyExists:
        pass

    db_session.set_keyspace(keyspace)
    connection.set_session(db_session)
    management.sync_table(City)

def main():
    sync_db()
    set_cql_logger()
    #from IPython import embed; embed()


if __name__ == '__main__':
    main()