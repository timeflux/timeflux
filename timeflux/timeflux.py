"""timeflux.timeflux: provides entry point main()."""

import signal
import sys
import os
import logging
from argparse import ArgumentParser
from importlib import import_module
from dotenv import load_dotenv
from timeflux import __version__
from timeflux.core.manager import Manager

LOGGER = logging.getLogger(__name__)
PID = os.getpid()

def main():
    LOGGER.info('Timeflux %s' % __version__)
    sys.path.append(os.getcwd())
    args = _args()
    _env(args.env)
    signal.signal(signal.SIGINT, _interrupt)
    _run_hook('pre')
    Manager(args.app).run()
    _terminate()

def _args():
    parser = ArgumentParser()
    parser.add_argument('-v', '--version', action='version', version='Timeflux %s' % __version__)
    parser.add_argument('-e', '--env', help='path to an environment file')
    parser.add_argument('app', help='path to the YAML or JSON application file')
    args = parser.parse_args()
    return args

def _env(path):
    load_dotenv(path)

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
