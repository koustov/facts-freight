from config import ConfigMap
from metadata import MetaData


def clear_tables():
    for table in ConfigMap.config()["tables"].tables:
        MetaData.target_connection_object.clear_table(table)


def drop_tables(table_list =None):
    for table in ConfigMap.config()["tables"].tables:
        if table_list != None or table.name in table_list:
            MetaData.target_connection_object.drop_table(table)
