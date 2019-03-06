"""timeflux.timeflux: provides entry point main()."""

import signal
import sys
import os
import logging
import argparse
from timeflux import __version__
from timeflux.core.manager import Manager

def main():
    sys.path.append(os.getcwd())
    args = _args()
    logging.info('Timeflux %s' % __version__)
    signal.signal(signal.SIGINT, _quit)
    Manager(args.config).run()

def _quit(signal, frame):
    logging.info('Interrupting')
    sys.exit(0)

def _args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version', action='version', version='Timeflux %s' % __version__)
    parser.add_argument('config', help='The config file or JSON string')
    args = parser.parse_args()
    return args
