#!/usr/bin/env python

""" Terminate a Timeflux instance

Use as a script or invoke with ``python -m timeflux.helpers.terminate``.

"""

import logging
import subprocess
import signal
import os

LOGGER = logging.getLogger(__name__)

def terminate_posix():
    """Find oldest Timeflux process and terminate it."""
    try:
        pid = int(subprocess.check_output(['pgrep', '-of', 'timeflux']))
        LOGGER.info('Sending INT signal to PID %d.' % pid)
        os.kill(pid, signal.SIGINT)
    except:
        LOGGER.warning('No running Timeflux instance found.')

if __name__ == '__main__': terminate_posix()
