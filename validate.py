import json
from log import Log
from os import walk
from config import ConfigMap, table_config
from metadata import MetaData
from constants import SchemaValidationStatus


class SchemaStat:
    column_not_found_at_source: "COLUMN_NOT_FOUND_AT_SOURCE"
    column_not_found_at_target: "COLUMN_NOT_FOUND_AT_TARGET"
    column_type_not_match: "COLUMN_TYPE_MISMATCH"
    column_length_not_match: "COLUMN_LENGTH_MISMATCH"
    primary_key_not_match: "PRIMARY_KEY_MISMATCH"
    final_result: "FINAL_RESULT"
    table_not_found_at_source: "TABLE_NOT_FOUND_AT_SOURCE"


class SchemaCheckReport:
    status = SchemaValidationStatus.not_started

    def __init__(self, table_config):
        self.check_time = MetaData.collection_start_time
        self.table_config = table_config
        self.table_name = table_config.name
        self.stat_details = []

    def is_table_exists_at_source(self, val):
        self.table_exits_at_source = val
        if val == False:
            Log.error(f"Source doesn't exists anymore", self.table_name)
            self.set_stat_details(
                "TABLE",
                self.table_name,
                SchemaStat.table_not_found_at_source,
                SchemaValidationStatus.error,
            )
        else:
            Log.info(f"Source table exists", self.table_name)
            self.set_status(SchemaValidationStatus.success)

    def is_source_column_same(self, meta_columns):
        print(">>>>>>>>>>>>>>>>>>")
        print(self.table_config.s_columns)
        print(">>>>>>>>>>>>>>>>>>")
        print(meta_columns)
        print(">>>>>>>>>>>>>>>>>>")
        if sorted(self.table_config.s_columns) == sorted(meta_columns):
            Log.info(f"Columns not changed", self.table_name)
        else:
            self.column_stat = []
            for t_col in self.table_config.s_columns:
                col_found = False
                for s_col in meta_columns:
                    if s_col["name"] == t_col["name"]:
                        col_found = True
                        if s_col["type"] != t_col["type"]:
                            self.set_stat_details(
                                "COLUMN",
                                t_col["name"],
                                SchemaStat.column_type_not_match,
                                SchemaValidationStatus.error,
                            )
                        if s_col["length"] != t_col["length"]:
                            self.set_stat_details(
                                "COLUMN",
                                t_col["name"],
                                SchemaStat.column_length_not_match,
                                SchemaValidationStatus.warning,
                            )

                if col_found == False:
                    self.set_stat_details(
                        "COLUMN",
                        t_col["name"],
                        SchemaStat.column_not_found_at_source,
                        SchemaValidationStatus.error,
                    )
            for s_col in meta_columns:
                col_found = False
                for t_col in self.table_config.s_columns:
                    if s_col["name"] == t_col["name"]:
                        col_found = True
                if col_found == False:
                    self.set_stat_details(
                        "COLUMN",
                        t_col["name"],
                        SchemaStat.column_not_found_at_target,
                        SchemaValidationStatus.error,
                    )
            if len(self.stat_details) > 0:
                Log.error(f"Column definition changed", self.table_name)

    def set_status(self, stat):
        self.status = stat if self.status >= stat else self.status

    def set_stat_details(self, type: str, name: str, stat_str: str, stat: int):
        self.stat_details.append(
            {
                "table_name": self.table_name,
                "type": type,
                "name": name,
                "stat_str": stat_str,
                "stat": stat,
            }
        )
        self.set_status(stat)

    def __str__(self):
        return self.stat_details

    def to_json(self):
        return self.stat_details


def validate(source_connection_object):
    Log.startblock("Schema Validation")

    table_count = 1
    total_table_count = len(ConfigMap.config()["tables"].tables)
    all_table_report = []
    for table in ConfigMap.config()["tables"].tables:
        Log.highlight(
            f"Validating {table_count} of {total_table_count} tables", table.name
        )
        schema_check_report = SchemaCheckReport(table)

        # Does source table exits
        if source_connection_object.table_exits(table) == True:
            schema_check_report.is_table_exists_at_source(True)
            table.t_columns = MetaData.load_table_from_meta(table)
            schema_check_report.is_source_column_same(table.t_columns)
        else:
            schema_check_report.is_table_exists_at_source(False)
        all_table_report.append(schema_check_report)
        table_count = table_count + 1
    final_stat = 0
    tables_to_recreate = []
    for report in all_table_report:
        for table_report in report.stat_details:
            final_result = (
                table_report["stat"]
                if final_result < table_report["stat"]
                else final_result
            )
            if final_result == SchemaValidationStatus.error:
                tables_to_recreate.append(report.table_name)
            MetaData.insert_schema_validation_row(table_report)

    return {"status": final_stat, "tables_to_recreate": tables_to_recreate}
