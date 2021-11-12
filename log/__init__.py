import json
from datetime import datetime

import sys
from termcolor import colored, cprint


class Log:
    log_directory = "logs"
    log_file = f"{datetime.now()}.log"
    log_file_path = f"{log_directory}/{log_file}"
    print_time_stamp = True
    color_map = {"INFO": "blue", "ERROR": "red", "WARNING": "yellow"}

    @staticmethod
    def setup(log_directory="logs", print_time_stamp=True):
        Log.log_directory = log_directory
        Log.log_file = f'{datetime.now().strftime("%d-%m-%Y %H-%M-%S")}.log'
        Log.log_file_path = f"{log_directory}/{Log.log_file}"
        Log.print_time_stamp = print_time_stamp

    @staticmethod
    def log(text, type):
        log_text = ""
        log_time = f'{log_text}{datetime.now().strftime("%H:%M:%S")}'
        log_type = type.upper()
        if Log.print_time_stamp == True:
            log_text = f"{log_time} : "
        if type is not None:
            log_text = f"{log_text}{type.upper()}:"
        else:
            log_text = f"{log_text}INFO: "
        log_text = f"{log_text}{text}"
        cprint(log_time, end=" ")
        cprint(log_type, Log.color_map[log_type], end=" ")
        cprint(":", end=" ")
        new_text = text
        if len(text) > 100:
            new_text = f"{text[0:100]}...[see details in log file:{Log.log_file} ]"
        cprint(new_text)
        with open(Log.log_file_path, "a+") as result_file:
            result_file.write(log_text + "\n")

    @staticmethod
    def info(text):
        Log.log(text, "INFO")

    @staticmethod
    def debug(text):
        Log.log(text, "DEBUG")

    @staticmethod
    def error(text):
        Log.log(text, "ERROR")

    @staticmethod
    def warning(text):
        Log.log(text, "WARNING")

    @staticmethod
    def startblock(text):
        print(
            f"-------------------------------------------Start: {text}-------------------------------------------"
        )

    @staticmethod
    def endblock(text):
        print(
            f"-------------------------------------------End: {text}-------------------------------------------"
        )
