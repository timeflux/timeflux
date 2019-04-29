#!/usr/bin/env python

""" Terminate any Timeflux instance

Use as a script or invoke with ``python -m timeflux.helpers.terminate``.

"""

import sys
import signal
import psutil

def terminate():
    # Get all processes
    process = None
    for p in psutil.process_iter(attrs=['cmdline']):
        if p.info['cmdline'] and len(p.info['cmdline']) > 2:
            if p.info['cmdline'][1].endswith('timeflux'):
                if not process:
                    process = p
                else:
                    # Keep only the master process
                    if p.ppid() != process.pid:
                        process = p
    # Send signal to the process
    if process:
        sig = signal.CTRL_C_EVENT if sys.platform == 'win32' else signal.SIGINT
        process.send_signal(sig)


if __name__ == '__main__': terminate()