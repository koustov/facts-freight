import json
from log import Log
from os import walk
from log import Log
import os
from .table_config import TableConfig, TableCollection
from os import listdir
from os.path import isfile, join


class ConfigMap:
    meta = None

    @staticmethod
    def config():
        if ConfigMap.meta == None:
            Log.info(f"Loading config information")
            # with open(f"{os.path.dirname(__file__)}/config_map.json") as json_data:
            with open(f"{os.path.dirname(__file__)}/meta.json") as json_data:
                ConfigMap.meta = json.load(json_data)

            table_config_path = f"{os.path.dirname(__file__)}/tables"
            all_tables_json = [
                f
                for f in listdir(table_config_path)
                if isfile(join(table_config_path, f))
            ]

            ConfigMap.meta["tables"] = TableCollection()
            for table_file in all_tables_json:
                file_path = f"{table_config_path}/{table_file}"
                with open(file_path) as json_data:
                    table_config = json.load(json_data)
                    ConfigMap.meta["tables"].add(TableConfig(table_config))
        return ConfigMap.meta
