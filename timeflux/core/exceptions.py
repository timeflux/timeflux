"""timeflux.core.exceptions: define exceptions"""

__all__ = [
    "WorkerInterrupt",
    "WorkerLoadError",
    "GraphDuplicateNode",
    "GraphUndefinedNode",
    "ValidationError",
]


class TimefluxException(Exception):
    """Generic Exception."""


class GraphDuplicateNode(TimefluxException):
    """Raised when a duplicate node is found."""

    def __init__(self, message, *args):
        self.message = message
        super().__init__(message, *args)


class GraphUndefinedNode(TimefluxException):
    """Raised when an undefined node is found in edges."""

    def __init__(self, message, *args):
        self.message = message
        super().__init__(message, *args)


class WorkerLoadError(TimefluxException):
    """Raised when a worker cannot be loaded."""

    def __init__(self, message, *args):
        self.message = message
        super().__init__(message, *args)


class WorkerInterrupt(TimefluxException):
    """Raised when a process stops prematurely."""

    def __init__(self, message="Interrupting", *args):
        self.message = message
        super().__init__(message, *args)


class ValidationError(TimefluxException):
    """Raised when input validation fails."""

    def __init__(self, param_name, message, *args):
        self.message = f"Validation error for param `{param_name}`: {message}"
        super().__init__(self.message, *args)
