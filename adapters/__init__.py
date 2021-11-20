# try:
#     from aurora import aurora
# except ImportError:
#     from .aurora import aurora

from .aurora import *
from .vertica import *
from .postgres import *


def getConnectionObject(name, host, port, user, password, dbname, schema, config):
    if name == "aurora":
        return AuroraConnection(host, port, user, password, dbname, schema, config)
    if name == "vertica":
        return VerticaConnection(host, port, user, password, dbname, schema, config)
    if name == "postgres":
        return PostgresConnection(host, port, user, password, dbname, schema, config)
