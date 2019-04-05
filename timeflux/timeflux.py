"""Timeflux entry point"""

import signal
import sys
import os
import logging
from logging.config import dictConfig
from argparse import ArgumentParser
from importlib import import_module
from dotenv import load_dotenv
from timeflux import __version__
from timeflux.core.manager import Manager

LOGGER = logging.getLogger(__name__)
PID = os.getpid()


def init_logs():
    level_styles = {'debug': {'color': 'white'}, 'info': {'color': 'cyan'}, 'warning': {'color': 'yellow'},
                    'error': {'color': 'red'}, 'critical': {'color': 'magenta'}}
    field_styles = {'asctime': {'color': 'blue'}, 'levelname': {'color': 'black', 'bright': True},
                    'processName': {'color': 'green'}}
    date_format = "%(asctime)s %(levelname)-10s %(module)-12s %(process)-8s %(processName)-16s %(message)s"

    # On windows, the colors will not work unless we initialize the console with colorama
    if sys.platform.startswith('win'):
        try:
            import colorama
            colorama.init()
        except ImportError:
            # On windows, without colorama, do not use the colors
            level_styles = {}
            field_styles = {}


    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'root': {
            'level': 'INFO',
            'handlers': ['console'],
        },
        'loggers': {
            'timeflux': {
                'level': os.getenv('TIMEFLUX_LOG_LEVEL', 'DEBUG'),
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
    dictConfig(logging_config)


def main():
    sys.path.append(os.getcwd())
    args = _args()
    load_dotenv(args.env)
    init_logs()
    LOGGER.info('Timeflux %s',  __version__)
    signal.signal(signal.SIGINT, _interrupt)
    _run_hook('pre')
    try:
        Manager(args.app).run()
    except Exception as error:
        LOGGER.error(error)
    _terminate()

def _args():
    parser = ArgumentParser()
    parser.add_argument('-v', '--version', action='version', version='Timeflux %s' % __version__)
    parser.add_argument('-e', '--env', help='path to an environment file')
    parser.add_argument('app', help='path to the YAML or JSON application file')
    args = parser.parse_args()
    return args

def _interrupt(signal, frame):
    LOGGER.info('Interrupting')
    _terminate()

def _terminate():
    if os.getpid() == PID:
        try:
            os.waitpid(-1, 0)
        except Exception:
            pass
        _run_hook('post')
        LOGGER.info('Terminated')
        logging.shutdown()
    sys.exit(0)

def _run_hook(name):
    module = os.getenv('TIMEFLUX_HOOK_' + name.upper())
    if module:
        LOGGER.info('Running %s hook' % name)
        _import(module)

def _import(module):
    try:
        import_module(module)
    except Exception as error:
        LOGGER.exception(error)
