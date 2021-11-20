import json
from datetime import datetime

import sys
import os
from termcolor import colored, cprint


class Log:
    log_directory = "logs"
    log_file = f'{datetime.now().strftime("%d-%m-%Y %H-%M-%S")}.log'
    log_file_path = f"{log_directory}/{log_file}"
    print_time_stamp = True
    color_map = {
        "INFO": "blue",
        "ERROR": "red",
        "WARNING": "red",
        "DEBUG": "yellow",
        "HIGHLIGHT": "green",
    }
    max_file_count = 5

    @staticmethod
    def setup(log_directory="logs", print_time_stamp=True, max_file_count=5):
        Log.log_directory = log_directory
        Log.log_file = f'{datetime.now().strftime("%d-%m-%Y %H-%M-%S")}.log'
        Log.log_file_path = f"{log_directory}/{Log.log_file}"
        Log.print_time_stamp = print_time_stamp
        Log.max_file_count = max_file_count

        list_of_files = os.listdir(Log.log_directory)
        full_path = ["logs/{0}".format(x) for x in list_of_files]
        if len(list_of_files) > Log.max_file_count:

            oldest_file = min(full_path, key=os.path.getctime)
            oldest_file_full_path = f"{oldest_file}"
            os.remove(oldest_file_full_path)

        # Creates a new file
        with open(Log.log_file_path, "w+") as fp:
            fp.write("Starting a new log file" + "\n")

    @staticmethod
    def log(text, type, block_name="General", show_stdout=True):
        log_text = ""
        log_time = f'{log_text}{datetime.now().strftime("%H:%M:%S")} : {os.getpid()} :'
        log_type = type.upper()
        if Log.print_time_stamp == True:
            log_text = f"{log_time} : "
        if type is not None:
            log_text = f"{log_text}{type.upper()}:"
        else:
            log_text = f"{log_text}INFO: "
        log_text = f"{log_text}{text}"
        if show_stdout:
            cprint(log_time, end=" ")
            cprint(log_type, Log.color_map[log_type], end=" ")
            if block_name is not "":
                cprint(f"[{block_name}]", end=" ")
            cprint(":", end=" ")
            new_text = text
            if len(text) > 100:
                new_text = f"{text[0:100]}...[see details in log file:{Log.log_file} ]"
            cprint(new_text)
        with open(Log.log_file_path, "a") as result_file:
            result_file.write(log_text + "\n")

    @staticmethod
    def info(text, block_name=""):
        Log.log(text, "INFO", block_name)

    @staticmethod
    def debug(text, block_name=""):
        Log.log(text, "DEBUG", block_name, False)

    @staticmethod
    def error(text, block_name=""):
        Log.log(text, "ERROR", block_name)

    @staticmethod
    def warning(text, block_name=""):
        Log.log(text, "WARNING", block_name)

    @staticmethod
    def highlight(text, block_name=""):
        Log.log(text, "HIGHLIGHT", block_name)

    @staticmethod
    def startblock(text):
        cprint(
            "-------------------------------------------",
            Log.color_map["HIGHLIGHT"],
            end=" ",
        )
        cprint(f"Start: {text}", Log.color_map["INFO"], end=" ")
        cprint(
            "-------------------------------------------",
            Log.color_map["HIGHLIGHT"],
            end=" ",
        )
        print("")

    @staticmethod
    def endblock(text):
        cprint(
            "-------------------------------------------",
            Log.color_map["HIGHLIGHT"],
            end=" ",
        )
        cprint(f"End: {text}", Log.color_map["INFO"], end=" ")
        cprint(
            "-------------------------------------------",
            Log.color_map["HIGHLIGHT"],
            end=" ",
        )
        print("")
