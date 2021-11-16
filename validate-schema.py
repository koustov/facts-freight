import json
from log import Log
from os import walk


def validate(source_connection_object, target_connection_object):
    table_count = 1
    for (dirpath, dirnames, filenames) in walk("./config/tables"):
        file_path = f"{dirpath}/{filenames[0]}"
        Log.info(f"Processing source definition: {table_count}: {filenames[0]}")
        with open(file_path) as json_data:
            table_config = json.load(json_data)

        table_count = table_count + 1
