import sys
from os import walk
import json
from log import Log


def create(source_connection_object, target_connection_object):
    allTables = []
    Log.info(f"Reading table information")
    count = 1
    for (dirpath, dirnames, filenames) in walk("./config/tables"):
        file_path = f"{dirpath}/{filenames[0]}"
        Log.info(f"Processing source definition: {count}: {filenames[0]}")
        with open(file_path) as json_data:
            table_config = json.load(json_data)
            # table_data = allTables.append(table_config)
            Log.info("Loading source table details")
            table_config["s_columns"] = source_connection_object.get_table_schema(
                table_config["name"], table_config
            )
            allTables.append(table_config)
            # print(table_config)
        count = count + 1

    for table in allTables:
        Log.info("Creating target tables")
        target_connection_object.create_table(table)
    return allTables
