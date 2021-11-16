import sys
import json
import os
import logging
from os import walk
from constants import Constants
from create_tables import create_and_update_table
from config import ConfigMap
from log import Log
from multiprocessing import Process
import time
import random
from metadata import MetaData

import adapters

timestamp = int(time.time())
# Initialize log
Log.setup("logs", True, ConfigMap.config()["log"]["max_file_count"])

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
    ConfigMap.config(),
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
    ConfigMap.config(),
)

Log.info(f"Target connection succeeded")
Log.info(
    f'Connection details: {json.dumps(ConfigMap.config()["target_server"], indent=4, sort_keys=True) }'
)
# Initialize metadata
MetaData.initialize(target_connection_object)
Log.endblock("Initialization")

# Check version
version_row = target_connection_object.execute_select_query(
    "version",
    MetaData.meta_data_overview_table_name,
    f"ORDER BY start_time DESC LIMIT 2",
)
old_version = ""
if version_row != None and len(version_row) > 1:
    old_version = version_row[1][0]
    if old_version != ConfigMap.config()["version"]:
        # Call schema check API
        Log.warning(
            f"Version updated. Previous: {old_version} , New : {ConfigMap.config()['version']}"
        )
        Log.warning(f"Calling schema validation now")
    else:
        Log.info("No version changed. Will not invoke schema validation")

# load table definitions
table_count = 1
for table_config in ConfigMap.get_tables():
    Log.info(f"Processing source definition: {table_count}: {table_config['name']}")
    p = Process(
        target=create_and_update_table,
        args=(
            source_connection_object,
            target_connection_object,
            table_config,
        ),
    )
    p.start()
    table_count = table_count + 1

MetaData.collection_end()
