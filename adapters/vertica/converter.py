def convert_data(val, type):
    try:
        if type == "varchar":
            if val == None:
                return "''"
            fval = (str(val)).replace("'", "''")
            return f"'{fval}'"
        if type == "number" or type == "bigint":
            if val == None:
                return 0
            return int(val)
        if type == "timestamp with time zone":
            if val == None:
                return "(%s)" % "NULL"
            return f"'{val}'"
        if type == "boolean":
            if val == None:
                return False
            return bool(val)
    except:
        print(f"Error converting type: {type}: {val}")
        fval = (str(val)).replace("'", "''")
        return f"'{fval}'"
