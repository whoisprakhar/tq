import os
import logging
import coloredlogs

log_file = "{}-log.log"

path = os.path.dirname(os.path.dirname(__file__))
path = os.path.join(path, "logs")

date_fmt = "MM-DD-YYY"
log_fmt = "%(asctime)s-%(levelname)s : %(message)s"
time_fmt = '%Y-%m-%d %H:%M:%S'

CGREENBG = '\33[42m'
CEND = '\33[1m'

loggers = {}


class BColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    DARKGREEN = '\033[32m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'



def get_logger(name):
    if name in loggers:
        return loggers.get(name)

    if not os.path.exists(path):
        os.mkdir(os.path.join(path))

    fname = os.path.join(path, log_file.format(name))
    handler = logging.FileHandler(fname)

    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter(log_fmt, time_fmt)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)

    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    loggers[name] = logger

    coloredlogs.install(level='DEBUG', logger=logger)
    return logger


def log(message, *args, color=BColors.DARKGREEN):
    print(color, message, BColors.ENDC, *args)


def info(message, *args):
    log(message, *args, color=BColors.OKBLUE)


def log_error(message, *args):
    log(message, *args, color=BColors.FAIL)


def success(message, *args):
    log(message, *args, color=BColors.DARKGREEN)
