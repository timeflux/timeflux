"""timeflux.core.exceptions: define exceptions"""

class TimefluxException(Exception):
    """Generic Exception."""

class WorkerInterrupt(TimefluxException):
    """Raised when a process stops prematurely."""
    def __init__(self, message, *args):
        self.message = message
        super().__init__(message, *args)
