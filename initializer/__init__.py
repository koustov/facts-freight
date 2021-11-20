import sys
import json
import os
import logging
from os import walk
from constants import Constants, SchemaValidationStatus
from create_tables import create_and_update_table_metadata
from config import ConfigMap
from log import Log
from multiprocessing import Process
import multiprocessing
import time
import random
from metadata import MetaData
from clean import drop_tables
from copy_tables import copy_data
from util import Util
from validate import validate

import adapters

def initialize():

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
        "Novell2002",
        ConfigMap.config()["source_server"]["database"],
        ConfigMap.config()["source_server"]["schema"],
        ConfigMap.config(),
    )
    Log.info(f"Source connection succeeded")
    Log.debug(
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

    config_connection_object = adapters.getConnectionObject(
        ConfigMap.config()["target"],
        ConfigMap.config()["target_server"]["server"],
        ConfigMap.config()["target_server"]["port"],
        ConfigMap.config()["target_server"]["user"],
        "control1234",
        ConfigMap.config()["target_server"]["database"],
        ConfigMap.config()["target_server"]["schema"],
        ConfigMap.config(),
    )

    Log.info(f"Config connection succeeded")


    Log.debug(
        f'Connection details: {json.dumps(ConfigMap.config()["target_server"], indent=4, sort_keys=True) }'
    )
    # Initialize metadata
    MetaData.initialize(
        target_connection_object, source_connection_object, config_connection_object
    )

    Log.info("Loading source table schema details")
    MetaData.load_all_table_schema()

    # drop_tables()

    print(MetaData)
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.")
    print(ConfigMap)
    print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.")


