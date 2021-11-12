def convert_data(val, type):
    if type == "varchar":
        return f"'{val}'"
    if type == "number":
        return int(val)
