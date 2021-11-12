# try:
#     from aurora import aurora
# except ImportError:
#     from .aurora import aurora

from .aurora import *
from .vertica import *


def getConnectionObject(name, host, port, user, password, dbname, schema):
    if name == "aurora":
        return AuroraConnection(host, port, user, password, dbname, schema)
    if name == "vertica":
        return VerticaConnection(host, port, user, password, dbname, schema)
