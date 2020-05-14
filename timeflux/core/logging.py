import os
import sys
import threading
import time
import logging
import logging.config
import logging.handlers
import coloredlogs
from multiprocessing import Queue
from datetime import datetime


_QUEUE = None


class UTCFormatterConsole(coloredlogs.ColoredFormatter):
    converter = time.gmtime


class UTCFormatterFile(logging.Formatter):
    converter = time.gmtime


class Handler:
    def __init__(self):
        self.logger = logging.getLogger("timeflux")

    def handle(self, record):
        self.logger.handle(record)


def init_listener(level_console="INFO", level_file="DEBUG", file=None):

    q = get_queue()

    level_styles = {
        "debug": {"color": "white"},
        "info": {"color": "cyan"},
        "warning": {"color": "yellow"},
        "error": {"color": "red"},
        "critical": {"color": "magenta"},
    }
    field_styles = {
        "asctime": {"color": "blue"},
        "levelname": {"color": "black", "bright": True},
        "processName": {"color": "green"},
    }
    record_format = "%(asctime)s %(levelname)-10s %(module)-12s %(process)-8s %(processName)-16s %(message)s"

    # On Windows, the colors will not work unless we initialize the console with colorama
    if sys.platform.startswith("win"):
        try:
            import colorama

            colorama.init()
        except ImportError:
            # On Windows, without colorama, do not use the colors
            level_styles = {}
            field_styles = {}

    # Define config
    config = {
        "version": 1,
        "root": {"handlers": ["default"],},
        "loggers": {
            "timeflux": {"propagate": False, "level": "DEBUG", "handlers": ["console"]},
        },
        "formatters": {
            "console": {
                "()": "timeflux.core.logging.UTCFormatterConsole",
                "format": record_format,
                "datefmt": "%Y-%m-%d %H:%M:%S,%f",
                "field_styles": field_styles,
                "level_styles": level_styles,
            },
            "file": {
                "class": "timeflux.core.logging.UTCFormatterFile",
                "format": record_format,
            },
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "formatter": "console",
                "stream": "ext://sys.stdout",
            },
            "console": {
                "level": level_console,
                "class": "logging.StreamHandler",
                "formatter": "console",
                "stream": "ext://sys.stdout",
            },
            "file": {
                "level": level_file,
                "class": "logging.FileHandler",
                "filename": "timeflux.log",
                "mode": "a",
                "delay": True,
                "formatter": "file",
            },
        },
    }

    if file:
        now = datetime.now()
        config["loggers"]["timeflux"]["handlers"].append("file")
        config["handlers"]["file"]["filename"] = now.strftime(file)

    logging.config.dictConfig(config)

    queue = get_queue()
    listener = logging.handlers.QueueListener(queue, Handler())
    listener.start()


def terminate_listener():
    if _QUEUE:
        _QUEUE.put_nowait(None)


def init_worker(queue):

    config = {
        "version": 1,
        "handlers": {
            "queue": {"class": "logging.handlers.QueueHandler", "queue": queue},
        },
        "loggers": {
            "timeflux": {"propagate": False, "level": "DEBUG", "handlers": ["queue"]}
        },
        "root": {"handlers": ["queue"]},
    }

    logging.config.dictConfig(config)


def get_queue():
    if not _QUEUE:
        globals()["_QUEUE"] = Queue()
    return _QUEUE
