""" Run a Python method in a lightweight background process.

Example:

    .. code-block:: python

        from timeflux.helpers.background import Task
        from my.module import MyClass

        task = Task(MyClass(), 'my_method', my_arg=42).start()
        while not task.done:
            status = task.status()
        print(status)

"""

import sys
import time
import logging
import traceback
import zmq
from subprocess import Popen


class Runner:

    """ Background base class. Provides common methods.

    .. warning::
        Do not use directly!

    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _send(self, data):
        try:
            self._socket.send_pyobj(data, copy=False)
        except zmq.ZMQError as e:
            self.logger.error(e)

    def _receive(self, blocking=True):
        flag = 0 if blocking else zmq.NOBLOCK
        try:
            return self._socket.recv_pyobj(flag)
        except zmq.ZMQError:
            pass  # No data


class Task(Runner):

    """ Background task.

    Launch a 0MQ PAIR server, start a client and dispatch the task.

    Attributes:
        done (bool): Indicates if the task is complete.

    Args:
        instance (object): A picklable class instance.
        method (string): The method name to call from the instance.
        *args: Arbitrary variable arguments to be passed to the method.
        **kwargs: Arbitrary keyword arguments to be passed to the method.

    """

    def __init__(self, instance, method, *args, **kwargs):
        super().__init__()
        context = zmq.Context()
        self._socket = context.socket(zmq.PAIR)
        self._port = self._socket.bind_to_random_port("tcp://127.0.0.1")
        self.done = False
        self.instance = instance
        self.method = method
        self.args = args
        self.kwargs = kwargs

    def start(self):
        """Run the task."""
        self._process = Popen(["python", "-m", __name__, str(self._port)])
        self._send(
            {
                "instance": self.instance,
                "method": self.method,
                "args": self.args,
                "kwargs": self.kwargs,
            }
        )
        return self

    def stop(self):
        """Terminate the task."""
        self._process.kill()
        self.done = True

    def status(self):
        """Get the task status.

        Returns:

            `None` if the task is not complete or a dict containing the following keys.

                - ``success``: A boolean indicating if the task ran successfully.
                - ``instance``: The (possibly modified) instance.
                - ``result``: The result of the method call, if `success` is `True`.
                - ``exception``: The exception, if `success` is `False`.
                - ``traceback``: The traceback, if `success` is `False`.
                - ``time``: The time it took to run the task.

        """
        response = self._receive(False)
        if response is not None:
            self.done = True
        return response


class Worker(Runner):

    """ Background worker. Connects to the server and executes the task.

    .. warning::
        Do not use directly!

    """

    def __init__(self, port):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        context = zmq.Context()
        self._socket = context.socket(zmq.PAIR)
        self._socket.connect(f"tcp://127.0.0.1:{port}")

    def execute(self):
        """Get the task from the socket and run it."""
        response = {}
        start = time.perf_counter()
        try:
            data = self._receive()
            result = getattr(data["instance"], data["method"])(
                *data["args"], **data["kwargs"]
            )
            response["instance"] = data["instance"]
            response["result"] = result
            response["success"] = True
        except Exception as e:
            response["exception"] = e
            response["traceback"] = traceback.format_tb(e.__traceback__)
            response["success"] = False
        response["time"] = time.perf_counter() - start
        self._send(response)


if __name__ == "__main__":

    if len(sys.argv) == 1:
        sys.exit()
    port = sys.argv[1]
    Worker(port).execute()
