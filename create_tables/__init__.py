import sys
from os import walk
import json
from log import Log
from copy_tables import update_data


def create_and_update_table(
    source_connection_object, target_connection_object, table_config
):
    table_name = table_config["name"]
    Log.info("Loading source table details", table_name)
    table_config["s_columns"] = source_connection_object.get_table_schema(
        table_name, table_config
    )

    # Creating target table
    Log.info("Creating or updating target table", table_name)
    target_connection_object.create_table(table_config)
    Log.info("Table created", table_name)

    # Copy data to target table
    Log.info("Data loading started", table_name)
    update_data(source_connection_object, target_connection_object, table_config)
    Log.info("Data loading over", table_name)
