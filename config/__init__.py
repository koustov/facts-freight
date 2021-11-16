import json
from log import Log
from os import walk
from log import Log
import os


class ConfigMap:
    meta_config = None
    tables = []

    @staticmethod
    def config():
        if ConfigMap.meta_config == None:
            Log.info(f"Loading config information")
            # with open(f"{os.path.dirname(__file__)}/config_map.json") as json_data:
            with open(f"{os.path.dirname(__file__)}/meta.json") as json_data:
                ConfigMap.meta_config = json.load(json_data)
        return ConfigMap.meta_config

    @staticmethod
    def get_tables():
        if len(ConfigMap.tables) == 0:

            ConfigMap.tables = []
            for (dirpath, dirnames, filenames) in walk(
                f"{os.path.dirname(__file__)}/tables"
            ):
                file_path = f"{dirpath}/{filenames[0]}"
                with open(file_path) as json_data:
                    table_config = json.load(json_data)
                    ConfigMap.tables.append(table_config)
        return ConfigMap.tables
