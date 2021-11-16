import sys
import os
from os import walk
import json
import csv
import time
from log import Log
from config import ConfigMap


class Util:
    @staticmethod
    def get_abs_table_name(table_name):
        final_name = table_name
        conf = ConfigMap.config()
        if "object_prefix" in conf:
            final_name = f"{conf['object_prefix']}{table_name}"
        if "object_suffix" in conf:
            final_name = f"{table_name}{conf['object_suffix']}"
        return final_name
