import sys
import json
import os
import logging
from os import walk
from create_tables import create_and_update_table
from config import ConfigMap
from log import Log
from multiprocessing import Process
import time
import random

import adapters

timestamp = int(time.time())

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
Log.endblock("Initialization")

# load table definitions
table_count = 1
for (dirpath, dirnames, filenames) in walk("./config/tables"):
    file_path = f"{dirpath}/{filenames[0]}"
    Log.info(f"Processing source definition: {table_count}: {filenames[0]}")
    with open(file_path) as json_data:
        table_config = json.load(json_data)
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


# all_tables = create(source_connection_object, target_connection_object)
# copy(source_connection_object, target_connection_object, all_tables, timestamp)


# def f(name):
#     list1 = [100, 200, 300, 400, 500, 600]
#     # print(random.choice(list1))
#     sleep_time = random.choice(list1)
#     print(f"Sleeping {sleep_time}")
#     time.sleep(sleep_time)
#     print("hello", name)


# if __name__ == "__main__":
#     for num in range(10):
#         p = Process(target=f, args=(num,))
#         p.start()
#         # p.join()
