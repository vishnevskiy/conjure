from .exceptions import ConnectionError
from pymongo.connection import Connection
from pymongo.uri_parser import parse_uri
import sys

_connections = {}

def _get_connection(hosts):
    global _connections

    hosts = ['%s:%d' % host for host in hosts]
    key = ','.join(hosts)
    connection = _connections.get(key)

    if connection is None:
        try:
            connection = _connections[key] = Connection(hosts, use_greenlets='greenlet' in sys.modules)
        except Exception:
            raise ConnectionError('Cannot connect to the Mongo')

    return connection

def connect(uri):
    parsed_uri = parse_uri(uri, Connection.PORT)

    hosts = parsed_uri['nodelist']
    username = parsed_uri['username']
    password = parsed_uri['password']
    database = parsed_uri['database']

    db = _get_connection(hosts)[database]

    if username and password:
        db.authenticate(username, password)

    return db