import sys
import json
import os
import logging
from os import walk
from create_tables import create
from copy_tables import copy
import time
from config import ConfigMap
from log import Log

import adapters

timestamp = int(time.time())

Log.setup("logs", True)
Log.startblock("Initialization")
Log.info(f"Getting source connection object")
source_connection_object = adapters.getConnectionObject(
    ConfigMap.config()["source"],
    ConfigMap.config()["source_server"]["server"],
    ConfigMap.config()["source_server"]["port"],
    ConfigMap.config()["source_server"]["user"],
    "Arcsight?123",
    ConfigMap.config()["source_server"]["database"],
    ConfigMap.config()["source_server"]["schema"],
)
Log.info(f"Source connection succeeded")
Log.info(
    f'Connection details: {json.dumps(ConfigMap.config()["source_server"], indent=4, sort_keys=True) }'
)

Log.info(f"Getting target connection object")

target_connection_object = adapters.getConnectionObject(
    ConfigMap.config()["target"],
    ConfigMap.config()["target_server"]["server"],
    ConfigMap.config()["target_server"]["port"],
    ConfigMap.config()["target_server"]["user"],
    "control1234",
    ConfigMap.config()["target_server"]["database"],
    ConfigMap.config()["target_server"]["schema"],
)

Log.info(f"Target connection succeeded")
Log.info(
    f'Connection details: {json.dumps(ConfigMap.config()["target_server"], indent=4, sort_keys=True) }'
)
Log.endblock("Initialization")
all_tables = create(source_connection_object, target_connection_object)
copy(source_connection_object, target_connection_object, all_tables, timestamp)
