import sys
import os
from os import walk
import json
import csv
import time


def copy(
    source_connection_object, target_connection_object, alltables, timestamp, config
):
    print(f"Reading table data")
    for table in alltables:
        str_columns = "*"
        if len(table["columns"]) > 0:
            str_columns = ",".join(table["columns"])

        # Reading values from source
        result = source_connection_object.select(table["name"], str_columns)
        dir_path = f"data/{table['name']}"
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        file_name = f"{timestamp}.csv"
        file_path = f"{dir_path}/{file_name}.csv"
        print(f"Writing to: {file_name}")
        with open(file_path, "w+") as result_file:

            csv_out = csv.writer(result_file)
            for row in result:
                new_row = row + (
                    timestamp,
                    timestamp,
                )
                csv_out.writerow(new_row)
        print(f"Written: {len(result)} entries")
        with open(file_path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=",")
            line_count = 0
            for row in csv_reader:
                # if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                target_connection_object.insert_values(table["name"], row)
                line_count += 1
                # else:
                #     print(
                #         f"\t{row[0]} works in the {row[1]} department, and was born in {row[2]}."
                #     )
                #     line_count += 1
            target_connection_object.finalize_insert()
            print(f"Processed {line_count} rows.")

        # with open(file_path) as csvfile:
        #     row_reader = csv.reader(csvfile)
        #     for row in row_reader:
        #         target_connection_object.insert_values(table["name"], ", ".join(row))
        #     target_connection_object.finalize_insert()
