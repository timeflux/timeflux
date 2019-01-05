"""timeflux.core.exceptions: define exceptions"""

class TimefluxException(Exception):
    """Generic Exception."""

class WorkerTerminated(TimefluxException):
    """Raised when a worker died prematurely."""
    def __init__(self, message, pid, *args):
        self.message = message
        self.pid = pid
        super(WorkerTerminated, self).__init__(message, pid, *args)

class WorkerInterrupt(TimefluxException):
    """Raised when a process stops prematurely."""
    def __init__(self, message, *args):
        self.message = message
        super(WorkerInterrupt, self).__init__(message, *args)
