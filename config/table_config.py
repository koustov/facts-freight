import uuid
import time
import json


class TableConfig:
    id = str(uuid.uuid4())
    name = ""
    type = ""
    columns = []
    uniqucolumns = []
    update_identification_column = []
    dependency = {}
    pk = ""
    s_columns = []
    t_columns = []
    table_object = {}
    rows = 0

    def __init__(self, table_object):
        self.id = str(uuid.uuid4())
        self.table_object = table_object
        self.name = table_object["name"]
        self.type = table_object["type"]
        self.columns = table_object["columns"] if "columns" in table_object else []
        self.uniqucolumns = table_object["uniqucolumns"]
        self.update_identification_column = (
            table_object["update_identification_column"]
            if "update_identification_column" in table_object
            else []
        )
        self.dependency = (
            TableConfigDependency(table_object["dependency"])
            if "dependency" in table_object
            else []
        )
        self.s_columns = (
            table_object["s_columns"] if "s_columns" in table_object else []
        )
        self.t_columns = []
        self.pk = ""

    def get_raw_data(self):
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "columns": self.columns,
            "uniqucolumns": self.uniqucolumns,
            "update_identification_column": self.update_identification_column,
            "s_columns": self.s_columns,
            "pk": self.pk,
        }

    def get_column_json(self):
        return json.dumps(self.s_columns)


class TableConfigDependency:
    def __init__(self, dependency):
        self.child = dependency["child"]
        self.sibling = dependency["sibling"]


class TableCollection:
    tables = []

    def __init__(self):
        self.tables = []

    def add(self, table_config: TableConfig):
        self.tables.append(table_config)

    def find_by_id(self, id, parent=None):
        source = []

        if parent == None:
            source = self.tables
        else:
            source = parent
        for t in source:
            if t.id == id:
                return t
            if len(t.dependency.child) > 0:
                found = self.find_by_id(id, t.dependency)
                if found != None:
                    return found
        return None

    def get_tables(self):
        return self.tables
