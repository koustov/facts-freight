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
from initializer import initialize

import adapters

timestamp = int(time.time())
# # Initialize log
# Log.setup("logs", True, ConfigMap.config()["log"]["max_file_count"])

# Log.startblock("Initialization")

# Log.info(f"Getting source connection object")
# source_connection_object = adapters.getConnectionObject(
#     ConfigMap.config()["source"],
#     ConfigMap.config()["source_server"]["server"],
#     ConfigMap.config()["source_server"]["port"],
#     ConfigMap.config()["source_server"]["user"],
#     "Novell2002",
#     ConfigMap.config()["source_server"]["database"],
#     ConfigMap.config()["source_server"]["schema"],
#     ConfigMap.config(),
# )
# Log.info(f"Source connection succeeded")
# Log.debug(
#     f'Connection details: {json.dumps(ConfigMap.config()["source_server"], indent=4, sort_keys=True) }'
# )
# Log.info(f"Getting target connection object")

# target_connection_object = adapters.getConnectionObject(
#     ConfigMap.config()["target"],
#     ConfigMap.config()["target_server"]["server"],
#     ConfigMap.config()["target_server"]["port"],
#     ConfigMap.config()["target_server"]["user"],
#     "control1234",
#     ConfigMap.config()["target_server"]["database"],
#     ConfigMap.config()["target_server"]["schema"],
#     ConfigMap.config(),
# )

# Log.info(f"Target connection succeeded")

# config_connection_object = adapters.getConnectionObject(
#     ConfigMap.config()["target"],
#     ConfigMap.config()["target_server"]["server"],
#     ConfigMap.config()["target_server"]["port"],
#     ConfigMap.config()["target_server"]["user"],
#     "control1234",
#     ConfigMap.config()["target_server"]["database"],
#     ConfigMap.config()["target_server"]["schema"],
#     ConfigMap.config(),
# )

# Log.info(f"Config connection succeeded")


# Log.debug(
#     f'Connection details: {json.dumps(ConfigMap.config()["target_server"], indent=4, sort_keys=True) }'
# )
# # Initialize metadata
# MetaData.initialize(
#     target_connection_object, source_connection_object, config_connection_object
# )

# Log.info("Loading source table schema details")
# MetaData.load_all_table_schema()

# # drop_tables()

# print(MetaData)
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.")
# print(ConfigMap)
# print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>.")

initialize()

# Check version
version_row = MetaData.target_connection_object.execute_select_query(
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
        validation_result = validate(MetaData.source_connection_object)
        validation_stat = validation_result["stat"]
        validation_error_tables = validation_result["tables_to_recreate"]
        if validation_stat > SchemaValidationStatus.warning:
            Log.error(
                f"Schema changes causing recreation of the {len(validation_error_tables)} tables"
            )
            drop_tables(validation_error_tables)
    else:
        Log.info("No version changed. Will not invoke schema validation")

# load table definitions
table_count = 1
total_table_count = len(ConfigMap.config()["tables"].tables)
Log.startblock("Table Creations")

# all_processes = [
#     multiprocessing.Process(
#         target=create_and_update_table_metadata,
#         args=(
#             source_connection_object,
#             target_connection_object,
#             table_config,
#         ),
#     )
#     for table_config in ConfigMap.config()["tables"].tables
# ]
# for p in all_processes:
#     p.start()

# for p in all_processes:
#     p.join()
for table_config in ConfigMap.config()["tables"].tables:
    Log.highlight(
        f"Processing source definition: {table_count}/{total_table_count}: {table_config.name}"
    )
    Log.info("Creating table")
    create_and_update_table_metadata(
        MetaData.source_connection_object,
        MetaData.target_connection_object,
        table_config,
    )
    table_count = table_count + 1

Log.startblock("Data Loading")
Log.highlight("Initiating data loading process")
table_count = 1
for table_config in ConfigMap.config()["tables"].tables:
    # Copy data to target table
    Log.info(f"Invoking process for : {table_config.name}", table_config.name)
    copy_data(
        MetaData.source_connection_object,
        MetaData.target_connection_object,
        table_config,
    )
    # p = Process(
    #     target=copy_data,
    #     args=(
    #         source_connection_object,
    #         target_connection_object,
    #         table_config,
    #     ),
    # )
    # p.start()
    table_count = table_count + 1

MetaData.collection_end()
