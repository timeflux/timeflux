"""Timeflux entry point"""

import sys
import os
import logging
from argparse import ArgumentParser
from runpy import run_module
from dotenv import load_dotenv
from timeflux import __version__
from timeflux.core.logging import init_listener, terminate_listener
from timeflux.core.manager import Manager

LOGGER = logging.getLogger(__name__)


def main():
    sys.path.append(os.getcwd())
    args = _args()
    _init_env(args.env_file, args.env)
    _init_logging(args.debug)
    LOGGER.info("Timeflux %s" % __version__)
    _run_hook("pre")
    try:
        Manager(args.app).run()
    except Exception as error:
        LOGGER.error(error)
    _terminate()


def _args():
    parser = ArgumentParser()
    parser.add_argument(
        "-v", "--version", action="version", version="Timeflux %s" % __version__
    )
    parser.add_argument(
        "-d",
        "--debug",
        default=False,
        action="store_true",
        help="enable debug messages",
    )
    parser.add_argument(
        "-E", "--env-file", default="./.env", help="path to an environment file"
    )
    parser.add_argument("-e", "--env", action="append", help="environment variables")
    parser.add_argument("app", help="path to the YAML or JSON application file")
    args = parser.parse_args()
    return args


def _terminate():
    _run_hook("post")
    LOGGER.info("Terminated")
    terminate_listener()
    sys.exit(0)


def _init_env(file, vars):
    load_dotenv(file)
    if vars is not None:
        for env in vars:
            if "=" in env:
                env = env.split("=", 1)
                os.environ[env[0]] = env[1]


def _init_logging(debug):
    level_console = (
        "DEBUG" if debug else os.getenv("TIMEFLUX_LOG_LEVEL_CONSOLE", "INFO")
    )
    level_file = os.getenv("TIMEFLUX_LOG_LEVEL_FILE", "DEBUG")
    file = os.getenv("TIMEFLUX_LOG_FILE", None)
    init_listener(level_console, level_file, file)


def _run_hook(name):
    module = os.getenv("TIMEFLUX_HOOK_" + name.upper())
    if module:
        LOGGER.info("Running %s hook" % name)
        try:
            run_module(module)
        except ImportError as error:
            LOGGER.error(error)
        except Exception as error:
            LOGGER.exception(error)
