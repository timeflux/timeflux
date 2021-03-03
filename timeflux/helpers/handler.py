#!/usr/bin/env python

"""Launch and terminate Timeflux instances on both POSIX and Windows systems.

Sometimes, it's okay to kill. But only if you do it gracefully, and give your victim a chance to say goodbye
to his or her children and let them commit suicide (again, with grace) upon hearing such news.

On POSIX systems, it's easy: just launch your process normally and terminate it by sending a SIGINT signal.

On Windows, well, that's another story. SIGINT can't be captured, and the only way is to send a CTRL+C event.
Any other signal (except CTRL+BREAK) will terminate without notice. Simple enough, you might say. Not quite.
It just happens that CTRL events can only be captured by processes attached to the current console. Which is
pretty useless in most cases. But do not abandon all hope! Here is a (hacky) solution: have a launcher script
that will start a simple TCP server and run your program. When a client connect to the server, it will send a
CTRL+C event to its subprocess.

Use this helper as a script or invoke with: ``python -m timeflux.helpers.handler [cmd] [args]``.

Example:
    Launching a Timeflux instance: ``python -m timeflux.helpers.handler launch timeflux -d foobar.yaml``

Example:
    Terminating a Timeflux instance gracefully: ``python -m timeflux.helpers.handler terminate``

Example:
    Running Timeflux from a batch script:

    .. code-block:: batch

       @echo off
       set ROOT=C:\\Users\\%USERNAME%\\Miniconda3
       call %ROOT%\\Scripts\\activate.bat %ROOT%
       call conda activate timeflux
       start python -m timeflux.helpers.handler launch timeflux -d foobar.yaml
       pause
       call python -m timeflux.helpers.handler terminate

References:
    * `Python issue 26350 <https://bugs.python.org/issue26350#msg260201>`_


"""

import sys
import subprocess
import signal
import os
import subprocess
import socket


def launch_posix(args):
    """Launch a subprocess and exit."""
    try:
        subprocess.Popen(args)
    except:
        _exit_with_error(f"Invalid arguments: {args}")


def launch_windows(args, port=10000):
    """Launch a subprocess and await connection to a TCP server."""
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setblocking(False)
        server.bind(("localhost", port))
        server.listen(1)
    except:
        _exit_with_error(f"Could not start server on port {port}.")
    try:
        # flags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
        flags = subprocess.HIGH_PRIORITY_CLASS
        process = subprocess.Popen(args, creationflags=flags)
    except:
        _exit_with_error(f"Invalid arguments: {args}")
    while True:
        try:
            # Kill the process if a connection is received
            server.accept()
            process.send_signal(signal.CTRL_C_EVENT)
            break
        except BlockingIOError:
            try:
                # Exit the loop is the process is already dead
                process.wait(0.1)
                break
            except:
                pass


def terminate_posix(match="timeflux"):
    """Find oldest Timeflux process and terminate it."""
    try:
        pid = int(subprocess.check_output(["pgrep", "-of", match]))
        print(f"Sending INT signal to PID {pid}.")
        os.kill(pid, signal.SIGINT)
    except:
        _exit_with_error("No running instance found.")


def terminate_windows(port=10000):
    """Terminate the Timeflux process by connecting to the TCP server."""
    client = socket.socket()
    try:
        client.connect(("localhost", port))
        client.close()
    except:
        _exit_with_error("No running instance found.")


def _exit_with_error(message):
    print(message)
    sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.exit()
    if sys.argv[1] == "launch":
        args = sys.argv[2:]
        if sys.platform == "win32":
            try:
                port = (int(sys.argv[2]),)
                args = args[1:]
            except ValueError:
                port = ()
            launch_windows(args, *port)
        else:
            launch_posix(args)
    if sys.argv[1] == "terminate":
        arg = (sys.argv[2],) if len(sys.argv) >= 3 else ()
        if sys.platform == "win32":
            terminate_windows(*arg)
        else:
            terminate_posix(*arg)
