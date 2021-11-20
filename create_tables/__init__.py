import sys
from os import walk
import json
from log import Log
from metadata import MetaData


def create_and_update_table_metadata(
    source_connection_object, target_connection_object, table_config
):
    # table_name = table_config.name
    # Log.info("Loading source table details", table_name)
    # source_schema = source_connection_object.get_table_schema(table_name, table_config)
    # table_config.s_columns = source_schema["columns"]
    # table_config.pk = source_schema["pk"]
    # if (
    #     table_config.pk != ""
    #     and table_config.pk != None
    #     and table_config.pk not in table_config.uniqucolumns
    # ):
    #     table_config.uniqucolumns.append(table_config.pk)

    # Creating target table
    Log.info("Creating or updating target table", table_config.name)
    target_connection_object.create_table(table_config)
    Log.info("Table created", table_config.name)
    MetaData.update_table_collection_start_info(table_config)
    Log.info("Metadata updated", table_config.name)
