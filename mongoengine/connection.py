import warnings
import pymongo
from pymongo import MongoClient, ReadPreference, uri_parser

__all__ = [
    'DEFAULT_CONNECTION_NAME',
    'ConnectionError',
    'connect',
    'disconnect',
    'get_connection',
    'get_db',
    'register_connection',
]


DEFAULT_CONNECTION_NAME = 'default'
DEFAULT_READ_PREFERENCE = ReadPreference.PRIMARY


class ConnectionError(Exception):
    pass


_connection_settings = {}
_connections = {}
_dbs = {}


def register_connection(
    alias,
    name,
    host='localhost',
    port=27017,
    is_slave=False,
    read_preference=DEFAULT_READ_PREFERENCE,
    slaves=None,
    username=None,
    password=None,
    uuidrepresentation=None,
    **kwargs
):
    """Add a connection.

    :param alias: the name that will be used to refer to this connection
        throughout MongoEngine
    :param name: the name of the specific database to use
    :param host: the host name of the :program:`mongod` instance to connect to
    :param port: the port that the :program:`mongod` instance is running on
    :param is_slave: whether the connection can act as a slave
      ** Deprecated in PyMongo 2.0.1+
    :param read_preference: The read preference for the collection
      ** Added in PyMongo 2.1
    :param slaves: a list of aliases of slave connections; each of these must
        be a registered connection that has :attr:`is_slave` set to ``True``
    :param username: username to authenticate with
    :param password: password to authenticate with
    :param kwargs: allow ad-hoc parameters to be passed into the pymongo driver

    """
    global _connection_settings

    conn_settings = {
        'name': name,
        'host': host,
        'port': port,
        'is_slave': is_slave,
        'slaves': slaves or [],
        'username': username,
        'password': password,
        'read_preference': read_preference
    }

    # Handle uri style connections
    if "://" in host:
        uri_dict = uri_parser.parse_uri(host)
        if uri_dict.get('database') is None:
            raise ConnectionError("If using URI style connection include "\
                                  "database name in string")
        conn_settings.update({
            'host': host,
            'name': uri_dict.get('database'),
            'username': uri_dict.get('username'),
            'password': uri_dict.get('password'),
            'read_preference': read_preference,
        })
        if "replicaSet" in host:
            conn_settings['replicaSet'] = True
        if "uuidrepresentation" in uri_dict:
            uuidrepresentation = uri_dict.get('uuidrepresentation')

    if uuidrepresentation is None:
        warnings.warn(
            "No uuidrepresentation is specified! Falling back to "
            "'pythonLegacy' which is the default for pymongo 3.x. "
            "For compatibility with other MongoDB drivers this should be "
            "specified as 'standard' or '{java,csharp}Legacy' to work with "
            "older drivers in those languages. This will be changed to "
            "'unspecified' in a future release.",
            DeprecationWarning,
            stacklevel=3,
        )
        uuidrepresentation = "pythonLegacy"
    conn_settings['uuidrepresentation'] = uuidrepresentation
    conn_settings.update(kwargs)
    _connection_settings[alias] = conn_settings


def disconnect(alias=DEFAULT_CONNECTION_NAME):
    global _connections
    global _dbs

    if alias in _connections:
        get_connection(alias=alias).close()
        del _connections[alias]
    if alias in _dbs:
        del _dbs[alias]


def get_connection(alias=DEFAULT_CONNECTION_NAME, reconnect=False):
    global _connections
    # Connect to the database if not already connected
    if reconnect:
        disconnect(alias)

    if alias not in _connections:
        if alias not in _connection_settings:
            msg = 'Connection with alias "%s" has not been defined' % alias
            if alias == DEFAULT_CONNECTION_NAME:
                msg = 'You have not defined a default connection'
            raise ConnectionError(msg)
        conn_settings = _connection_settings[alias].copy()

        if hasattr(pymongo, 'version_tuple'):  # Support for 2.1+
            conn_settings.pop('name', None)
            conn_settings.pop('slaves', None)
            conn_settings.pop('is_slave', None)
        else:
            # Get all the slave connections
            if 'slaves' in conn_settings:
                slaves = []
                for slave_alias in conn_settings['slaves']:
                    slaves.append(get_connection(slave_alias))
                conn_settings['slaves'] = slaves
                conn_settings.pop('read_preference', None)

        if 'replicaSet' in conn_settings:
            conn_settings['hosts_or_uri'] = conn_settings.pop('host', None)
            # Discard port since it can't be used with replicaSet
            conn_settings.pop('port', None)
            # Discard replicaSet if not base string
            if not isinstance(conn_settings['replicaSet'], str):
                conn_settings.pop('replicaSet', None)

        try:
            _connections[alias] = MongoClient(**conn_settings)
        except Exception as e:
            raise ConnectionError("Cannot connect to database %s :\n%s" % (alias, e))
    return _connections[alias]


def get_db(alias=DEFAULT_CONNECTION_NAME, reconnect=False):
    global _dbs
    if reconnect:
        disconnect(alias)

    if alias not in _dbs:
        conn = get_connection(alias)
        conn_settings = _connection_settings[alias]
        db = conn[conn_settings['name']]
        _dbs[alias] = db
    return _dbs[alias]


def connect(db, alias=DEFAULT_CONNECTION_NAME, **kwargs):
    """Connect to the database specified by the 'db' argument.

    Connection settings may be provided here as well if the database is not
    running on the default port on localhost. If authentication is needed,
    provide username and password arguments as well.

    Multiple databases are supported by using aliases.  Provide a separate
    `alias` to connect to a different instance of :program:`mongod`.

    .. versionchanged:: 0.6 - added multiple database support.
    """
    global _connections
    if alias not in _connections:
        register_connection(alias, db, **kwargs)

    return get_connection(alias)


# Support old naming convention
_get_connection = get_connection
_get_db = get_db
