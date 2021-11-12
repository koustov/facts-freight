import sys
import boto3
import os
import psycopg2
import json
from log import Log
import math


class AuroraConnection:
    def __init__(self, host, port, user, password, dbname, schema, config):
        self.connection = psycopg2.connect(
            f"dbname={dbname} user={user} host={host} password={password} port={port}"
        )
        self.cur = self.connection.cursor()
        self.schema = schema
        self.global_config = config
        with open(f"{os.path.dirname(__file__)}/config_map.json") as json_data:
            self.config_map = json.load(json_data)
        Log.info("Aurora configuration initialized")

    def __get_mapped_data(self, val):
        # TODO: Not a suitable way. use has_key instead
        try:
            return self.config_map[val]
        except:
            return val

    def __get_mapped_data_type(self, val):
        # TODO: Not a suitable way. use has_key instead
        try:
            for dt in self.config_map["datatypes"]:
                if dt["source"] == val:
                    return dt["target"]
        except:
            return val

    def get_table_schema(self, tablename, table_config={"columns": []}):
        query = f"""SELECT column_name, is_nullable , data_type, character_maximum_length FROM  information_schema.columns WHERE table_name = '{tablename}'"""
        if len(table_config["columns"]) > 0:
            columns = ", ".join("'" + item + "'" for item in table_config["columns"])
            query = f"{query} AND column_name in ({columns})"
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        final_result = []
        for qr in query_results:
            final_result.append(
                {
                    "name": self.__get_mapped_data(qr[0]),
                    "nullable": self.__get_mapped_data(qr[1]),
                    "type": self.__get_mapped_data_type(qr[2]),
                    "length": self.__get_mapped_data(qr[3]),
                }
            )
        return final_result

    def execute(self, query):
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        return query_results

    def select(self, table_config):
        table_name = table_config["name"]
        Log.info(f"Reading {self.schema}.{table_name} meta information...", table_name)
        query = f"""SELECT count(*)  FROM  {self.schema}.{table_name}"""
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        total_rows = query_results[0][0]
        Log.info(f"Total rows {total_rows}", table_name)
        step_count = math.ceil(total_rows / self.global_config["bucket_size"])
        columns = ""
        if len(table_config["s_columns"]) > 0:
            col_collection = []
            for col in table_config["s_columns"]:
                print(col["name"])
                col_collection.append(col["name"])
            columns = ", ".join(col_collection)
        else:
            columns = "*"
        for step in range(step_count):
            Log.info(
                f"Batch size: {self.global_config['bucket_size']}, Processing {step + 1} of {step_count} batch",
                table_name,
            )
            query = f"""SELECT {columns} FROM  {self.schema}.{table_name} LIMIT {self.global_config['bucket_size']} OFFSET {self.global_config['bucket_size'] * step}"""
            Log.debug(f"Select query: {query}", table_name)
            self.cur.execute(query)
            query_results = self.cur.fetchall()
            Log.debug(
                f"Query result: {json.dumps(query_results, indent=4, sort_keys=True) }"
            )
            yield query_results
