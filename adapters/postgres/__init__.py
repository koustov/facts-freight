import sys
import boto3
import os
import psycopg2
import json
from log import Log
import math


class PostgresConnection:
    def __init__(self, host, port, user, password, dbname, schema, config):
        self.connection = psycopg2.connect(
            f"dbname={dbname} user={user} host={host} password={password} port={port}"
        )
        self.server = host
        self.cur = self.connection.cursor()
        self.schema = schema
        self.global_config = config
        self.test_connection()
        with open(f"{os.path.dirname(__file__)}/config_map.json") as json_data:
            self.config_map = json.load(json_data)
        Log.info("Aurora configuration initialized")

    def test_connection(self):
        self.cur.execute("""SELECT count(*) FROM information_schema.tables""")
        query_results = self.cur.fetchall()
        if query_results == None or len(query_results) == 0:
            raise Exception(f"Unable to connect to to postgres server: {self.server}")

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
                else:
                    return val
        except:
            return val

    def get_table_schema(self, tablename, table_config={"columns": []}):
        query = f"""SELECT column_name, is_nullable , data_type, character_maximum_length FROM  information_schema.columns WHERE table_name = '{tablename}'"""
        if len(table_config.columns) > 0:
            columns = ", ".join("'" + item + "'" for item in table_config.columns)
            query = f"{query} AND column_name in ({columns})"

        query_results_is = self.execute(query)
        final_result = []
        for qr in query_results_is:

            final_result.append(
                {
                    "name": self.__get_mapped_data(qr[0]),
                    "nullable": self.__get_mapped_data(qr[1]),
                    "type": self.__get_mapped_data_type(qr[2]),
                    "length": self.__get_mapped_data(qr[3]),
                }
            )

        query = f"""SELECT c.column_name
        FROM information_schema.table_constraints tc 
        JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) 
        JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
        AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
        WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '{tablename}'"""
        query_results_pk = self.execute(query)

        return {
            "columns": final_result,
            "pk": query_results_pk[0][0] if len(query_results_pk) > 0 else "",
        }

    def execute(self, query):
        Log.debug(query)
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        return query_results

    def select(self, table_config):
        Log.info(
            f"Reading {self.schema}.{table_config.name} meta information...",
            table_config.name,
        )
        query = f"""SELECT count(*)  FROM  {self.schema}.{table_config.name}"""
        self.cur.execute(query)
        query_results = self.cur.fetchall()
        total_rows = query_results[0][0]
        Log.info(f"Total rows {total_rows}", table_config.name)
        step_count = math.ceil(total_rows / self.global_config["bucket_size"])
        columns = ""
        if len(table_config.s_columns) > 0:
            col_collection = []
            for col in table_config.s_columns:
                col_collection.append(col["name"])
            columns = ", ".join(col_collection)
        else:
            columns = "*"
        for step in range(step_count):
            Log.info(
                f"Batch size: {self.global_config['bucket_size']}, Processing {step + 1} of {step_count} batch",
                table_config.name,
            )
            query = f"""SELECT {columns} FROM  {self.schema}.{table_config.name} LIMIT {self.global_config['bucket_size']} OFFSET {self.global_config['bucket_size'] * step}"""
            Log.debug(f"Select query: {query}", table_config.name)
            self.cur.execute(query)
            query_results = self.cur.fetchall()
            Log.debug(f"Retrieved {len(query_results)} rows", table_config.name)
            yield query_results

    def table_exits(self, table):
        query = f"""select count(*) from information_schema.tables where table_name = '{table.name}' and table_schema = '{self.schema}'"""
        query_results = self.execute(query)
        try:
            if query_results[0][0] == 1:
                return True
            else:
                return False
        except:
            return False
