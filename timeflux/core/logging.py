import os
import sys
import threading
import logging
from logging.config import dictConfig
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Queue

queue = Queue()
_thread = None

def listener(queue):
    root = logging.getLogger()
    while True:
        message = queue.get()
        if message is None:
            break
        root.handle(message)

def init_listener(level):

    level_styles = {'debug': {'color': 'white'}, 'info': {'color': 'cyan'}, 'warning': {'color': 'yellow'},
                    'error': {'color': 'red'}, 'critical': {'color': 'magenta'}}
    field_styles = {'asctime': {'color': 'blue'}, 'levelname': {'color': 'black', 'bright': True},
                    'processName': {'color': 'green'}}
    date_format = '%(asctime)s %(levelname)-10s %(module)-12s %(process)-8s %(processName)-16s %(message)s'

    # On Windows, the colors will not work unless we initialize the console with colorama
    if sys.platform.startswith('win'):
        try:
            import colorama
            colorama.init()
        except ImportError:
            # On Windows, without colorama, do not use the colors
            level_styles = {}
            field_styles = {}

    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'root': {
            'level': 'INFO',
            'handlers': ['console'],
        },
        'loggers': {
            'timeflux': {
                'level': level,
            },
        },
        'formatters': {
            'colored_console': {
                '()': 'coloredlogs.ColoredFormatter',
                'format': date_format,
                'datefmt': '%Y-%m-%d %H:%M:%S,%f',
                'field_styles': field_styles,
                'level_styles': level_styles,
            },
            'detailed': {
                'class': 'logging.Formatter',
                'format': '%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s'
            }
        },
        'handlers': {
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'formatter': 'colored_console',
                'stream': 'ext://sys.stdout',
            },
        }
    }
    dictConfig(config)

    # Start thread
    globals()['_thread'] = threading.Thread(target=listener, args=(queue,))
    globals()['_thread'].start()


def terminate_listener():
    if _thread:
        queue.put_nowait(None)

def init_sender(queue):
    root = logging.getLogger()
    root.handlers = []
    handler = QueueHandler(queue)
    root.addHandler(handler)
