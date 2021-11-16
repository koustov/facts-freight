import time
from config import ConfigMap
import uuid
from log import Log
from util import Util

status_values = {
    "not_started": 10,
    "queued": 20,
    "started": 30,
    "in_progress": 40,
    "finished": 50,
    "warning": 60,
    "error": 70,
}
meta_data_overview_table_object = {
    "name": "meta_data_overview",
    "type": "t",
    "s_columns": [
        {"type": "varchar", "name": "id", "length": 40},
        {"type": "number", "name": "start_time"},
        {"type": "number", "name": "end_time"},
        {"type": "number", "name": "status"},
        {"type": "varchar", "name": "version", "length": 11},
    ],
    "uniqucolumns": ["id"],
}
meta_data_collection_details_table_object = {
    "name": "meta_data_collection_details",
    "type": "t",
    "s_columns": [
        {"type": "varchar", "name": "id", "length": 40},
        {"type": "number", "name": "start_time"},
        {"type": "number", "name": "end_time"},
        {"type": "number", "name": "success"},
        {"type": "varchar", "name": "version", "length": 11},
    ],
    "uniqucolumns": ["id"],
}


class MetaData:
    collection_start_time = int(time.time())
    collection_end_time = int(time.time())
    processed_tables = []
    target_connection_object = {}
    collection_id = str(uuid.uuid4())
    meta_data_overview_table_name = Util.get_abs_table_name(
        meta_data_overview_table_object["name"]
    )
    meta_data_collection_details_table_object_table_name = Util.get_abs_table_name(
        meta_data_collection_details_table_object["name"]
    )

    @staticmethod
    def initialize(target_connection_object):
        Log.info("Initializing meta data")
        MetaData.collection_start_time = int(time.time())
        MetaData.collection_end_time = int(time.time())
        MetaData.processed_tables = []
        MetaData.target_connection_object = target_connection_object
        MetaData.collection_id = str(uuid.uuid4())
        MetaData.meta_data_overview_table_name = Util.get_abs_table_name(
            meta_data_overview_table_object["name"]
        )
        MetaData.meta_data_collection_details_table_object_table_name = (
            Util.get_abs_table_name(meta_data_collection_details_table_object["name"])
        )
        MetaData.create_metadata_table()

    @staticmethod
    def collection_end():
        MetaData.target_connection_object.insert_values(
            meta_data_overview_table_object,
            [
                MetaData.collection_id,
                MetaData.collection_start_time,
                int(time.time()),
                status_values["finished"],
                ConfigMap.config()["version"],
            ],
            True,
        )
        MetaData.target_connection_object.finalize_insert()

    def create_metadata_table():

        # Creating meta_data_overview_table_object
        Log.info("Creating meta_data_overview_table_object table")
        MetaData.target_connection_object.create_table(
            meta_data_overview_table_object, True
        )

        Log.info("Creating meta_data_collection_details_table_object table")
        # Creating meta_data_collection_details_table_object
        MetaData.target_connection_object.create_table(
            meta_data_collection_details_table_object, True
        )

        Log.info("Inserting collection start entry")
        # Inserting collection start
        MetaData.target_connection_object.insert_values(
            meta_data_overview_table_object,
            [
                MetaData.collection_id,
                MetaData.collection_start_time,
                0,
                status_values["queued"],
                ConfigMap.config()["version"],
            ],
            True,
        )
        MetaData.target_connection_object.finalize_insert()
