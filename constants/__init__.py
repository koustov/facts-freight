class Constants:
    collection_time_stamp_column_name = "dl_row_collection_ts"
    modification_time_stamp_column_name = "dl_row_modification_ts"

class Status:
    not_started = 10
    queued = 20
    started = 30
    in_progress = 40
    finished = 50
    warning = 60
    error = 70
class SchemaValidationStatus:
    not_started = 0
    success = 10
    warning = 0
    error = 20