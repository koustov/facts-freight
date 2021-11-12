import json
from log import Log


class ConfigMap:
    meta_config = None

    @staticmethod
    def config():
        if ConfigMap.meta_config == None:
            Log.info(f"Loading config information")
            with open("./config/meta.json") as json_data:
                ConfigMap.meta_config = json.load(json_data)
        return ConfigMap.meta_config
