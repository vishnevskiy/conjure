from .exceptions import ConnectionError
from pymongo import MongoClient
from pymongo.uri_parser import parse_uri

_connections = {}


def _get_connection(hosts):
    global _connections

    hosts = ['%s:%d' % host for host in hosts]
    key = ','.join(hosts)
    connection = _connections.get(key)

    if connection is None:
        try:
            connection = _connections[key] = MongoClient(hosts)
        except Exception as e:
            raise ConnectionError(e.message)

    return connection


def connect(uri):
    parsed_uri = parse_uri(uri, 27017)

    hosts = parsed_uri['nodelist']
    username = parsed_uri['username']
    password = parsed_uri['password']
    database = parsed_uri['database']

    db = _get_connection(hosts)[database]

    if username and password:
        db.authenticate(username, password)

    return db