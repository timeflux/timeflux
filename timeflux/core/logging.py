import os
import sys
import threading
import logging
import logging.config
import logging.handlers
from multiprocessing import Queue


_QUEUE = None


class Handler():

    def __init__(self):
        self.logger = logging.getLogger()

    def handle(self, record):
        self.logger.handle(record)


def init_listener(level='DEBUG'):

    q = get_queue()

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

    # Define config
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

    logging.config.dictConfig(config)

    queue = get_queue()
    listener = logging.handlers.QueueListener(queue, Handler())
    listener.start()


def terminate_listener():
    if _QUEUE:
        _QUEUE.put_nowait(None)


def init_worker(queue, level='DEBUG'):

    config = {
        'version': 1,
        'disable_existing_loggers': True,
        'handlers': {
            'queue': {
                'class': 'logging.handlers.QueueHandler',
                'queue': queue,
            },
        },
        'loggers': {
            'timeflux': {
                'level' : level
            }
        },
        'root': {
            'level': 'INFO',
            'handlers': ['queue']
        }
    }

    logging.config.dictConfig(config)


def get_queue():
    if not _QUEUE:
        globals()['_QUEUE'] = Queue()
    return _QUEUE
