import sys
import os
from os import walk
import json
import csv
import time
from log import Log


def update_data(source_connection_object, target_connection_object, table):
    Log.info(f"Reading table data {table}")
    table_name = table["name"]
    query_result = source_connection_object.select(table)
    row_count = 0
    for result in query_result:
        row_count = row_count + len(result)
        target_connection_object.insert_bulk_values(table, result)
    Log.info(f"Inserted {target_connection_object.insert_row_count} rows", table_name)
