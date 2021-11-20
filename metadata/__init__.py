import time
from config import ConfigMap
import uuid
from config.table_config import TableConfig
from log import Log
from util import Util
from constants import Status


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

meta_data_table_details_table_object = {
    "name": "meta_data_table_details",
    "type": "t",
    "s_columns": [
        {"type": "varchar", "name": "id", "length": 40},
        {"type": "varchar", "name": "table_name", "length": 250},
        {"type": "number", "name": "row_count"},
        {"type": "varchar", "name": "table_columns", "length": 2000},
        {"type": "varchar", "name": "primary_key", "length": 250},
        {"type": "number", "name": "start_time"},
        {"type": "number", "name": "end_time"},
        {"type": "number", "name": "collection_status"},
        {"type": "number", "name": "last_update_value"},
    ],
    "uniqucolumns": ["id"],
}

meta_data_schema_validation_details_table_object = {
    "name": "meta_data_schema_validation_details",
    "type": "t",
    "s_columns": [
        {"type": "varchar", "name": "id", "length": 40},
        {"type": "varchar", "name": "table_name", "length": 250},
        {"type": "varchar", "name": "type", "length": 250},
        {"type": "varchar", "name": "name", "length": 250},
        {"type": "varchar", "name": "stat_str", "length": 30},
        {"type": "number", "name": "stat"},
        {"type": "number", "name": "time"},
    ],
    "uniqucolumns": ["id"],
}


class MetaData:
    collection_start_time = int(time.time())
    collection_end_time = int(time.time())
    processed_tables = []
    target_connection_object = {}
    collection_id = str(uuid.uuid4())
    overview_table = {}
    meta_data_overview_table_name = Util.get_abs_table_name(
        meta_data_overview_table_object["name"]
    )
    meta_data_collection_details_table_object_table_name = Util.get_abs_table_name(
        meta_data_collection_details_table_object["name"]
    )

    meta_data_table_details_table_object = Util.get_abs_table_name(
        meta_data_table_details_table_object["name"]
    )

    meta_data_schema_validation_details_table_object = Util.get_abs_table_name(
        meta_data_schema_validation_details_table_object["name"]
    )

    source_connection_object = {}
    target_connection_object = {}
    config_connection_object = {}

    @staticmethod
    def initialize(
        target_connection_object, source_connection_object, config_connection_object
    ):
        Log.info("Initializing meta data")
        MetaData.collection_start_time = int(time.time())
        MetaData.collection_end_time = int(time.time())
        MetaData.processed_tables = []
        MetaData.target_connection_object = target_connection_object
        MetaData.source_connection_object = source_connection_object
        MetaData.collection_id = str(uuid.uuid4())

        MetaData.overview_table = TableConfig(meta_data_overview_table_object)
        MetaData.collection_details_table = TableConfig(
            meta_data_collection_details_table_object
        )

        MetaData.table_details_table = TableConfig(meta_data_table_details_table_object)
        MetaData.table_schema_validation_details_table = TableConfig(
            meta_data_schema_validation_details_table_object
        )
        MetaData.config_connection_object = config_connection_object
        MetaData.create_metadata_table()

    @staticmethod
    def collection_end():
        MetaData.target_connection_object.insert_values(
            MetaData.overview_table,
            [
                MetaData.collection_id,
                MetaData.collection_start_time,
                int(time.time()),
                Status.finished,
                ConfigMap.config()["version"],
            ],
            True,
        )
        MetaData.target_connection_object.finalize_insert(MetaData.overview_table)

    def create_metadata_table():

        # Creating meta_data_overview_table_object
        Log.info("Creating meta_data_overview_table_object table")
        MetaData.target_connection_object.create_table(MetaData.overview_table, True)

        Log.info("Creating meta_data_collection_details_table_object table")
        # Creating meta_data_collection_details_table_object
        MetaData.target_connection_object.create_table(
            MetaData.collection_details_table, True
        )

        # Creating meta_data_collection_details_table_object
        MetaData.target_connection_object.create_table(
            MetaData.collection_details_table, True
        )

        # Creating meta_data_table_details_table_object
        MetaData.target_connection_object.create_table(
            MetaData.table_details_table, True
        )

        # Creating meta_data_schema_validation_details
        MetaData.target_connection_object.create_table(
            MetaData.table_schema_validation_details_table, True
        )

        Log.info("Inserting collection start entry")
        # Inserting collection start
        MetaData.target_connection_object.insert_values(
            MetaData.overview_table,
            [
                MetaData.collection_id,
                MetaData.collection_start_time,
                0,
                Status.queued,
                ConfigMap.config()["version"],
            ],
            True,
        )
        MetaData.target_connection_object.finalize_insert(MetaData.overview_table)

    @staticmethod
    def update_table_collection_start_info(table_object: TableConfig):
        MetaData.target_connection_object.insert_values(
            MetaData.table_details_table,
            [
                str(uuid.uuid4()),
                table_object.name,
                table_object.rows,
                table_object.get_column_json(),
                table_object.pk,
                int(time.time()),
                0,
                Status.started,
            ],
            True,
        )

    @staticmethod
    def load_table_from_meta(table_object: TableConfig):
        query_result = MetaData.target_connection_object.execute(
            f"""SELECT * FROM {MetaData.target_connection_object.schema}.{Util.get_abs_table_name(MetaData.table_details_table.name)} WHERE table_name='{table_object.name}'"""
        )

        return query_result

    @staticmethod
    def load_table_schema(table_config):
        Log.info("Loading source table details", table_config.name)
        source_schema = MetaData.source_connection_object.get_table_schema(
            table_config.name, table_config
        )
        table_config.s_columns = source_schema["columns"]
        table_config.pk = source_schema["pk"]
        if (
            table_config.pk != ""
            and table_config.pk != None
            and table_config.pk not in table_config.uniqucolumns
        ):
            table_config.uniqucolumns.append(table_config.pk)

    @staticmethod
    def load_all_table_schema():
        for table in ConfigMap.config()["tables"].tables:
            MetaData.load_table_schema(table)

    @staticmethod
    def insert_schema_validation_row(report):
        # Inserting collection start
        MetaData.target_connection_object.insert_values(
            MetaData.table_schema_validation_details_table,
            [
                str(uuid.uuid4()),
                report.table_name,
                report.type,
                report.name,
                report.stat_str,
                report.stat,
                int(time.time()),
            ],
            True,
        )
