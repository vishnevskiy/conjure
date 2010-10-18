from mongoalchemy.exceptions import ConnectionError
from pymongo.connection import Connection, _parse_uri

_connections = {}

def _get_connection(hosts):
    global _connections

    hosts = ['%s:%d' % host for host in hosts]
    key = ','.join(hosts)
    connection = _connections.get(key)

    if connection is None:
        try:
            connection = _connections[key] = Connection(hosts)
        except Exception:
            raise ConnectionError('Cannot connect to the Mongo')

    return connection

def connect(uri):
    hosts, database, username, password = _parse_uri(uri, Connection.PORT)

    db = _get_connection(hosts)[database]

    if username and password:
        db.authenticate(username, password)

    return db