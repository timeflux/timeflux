"""Node base class."""

import re
import logging
from abc import ABC, abstractmethod
from timeflux.core.io import Port


class Node(ABC):
    def __new__(cls, *args, **kwargs):
        """Create instance and initialize the logger."""

        instance = super().__new__(cls)
        instance.logger = logging.getLogger(
            "timeflux." + cls.__module__ + "." + cls.__name__
        )
        instance.ports = {}
        return instance

    def __init__(self):
        """Instantiate the node."""

        pass

    def __getattr__(self, name):
        """Create input and output ports on the fly.

        Args:
            name (string): The name of the port, prefixed with `i_` (input) or `o_` (output).
                           Default ports are also allowed (`i` or `o`).

        Returns:
            Port: The newly created port.

        """

        if name == "i" or name.startswith("i_") or name == "o" or name.startswith("o_"):
            self.ports[name] = Port()
            setattr(self, name, self.ports[name])
            return self.ports[name]
        raise AttributeError(
            f"type object '{type(self).__name__}' has no attribute '{name}'"
        )

    def bind(self, source, target):
        """Create an alias of a port

        Args:
            source (string): The name of the source port
            target (string): The name of the target port

        """

        if (
            target == "i"
            or target.startswith("i_")
            or target == "o"
            or target.startswith("o_")
        ):
            getattr(self, source)  # Create the source port if it does not already exist
            self.ports[target] = self.ports[source]
            setattr(self, target, self.ports[source])

    def iterate(self, name="*"):
        """Iterate through ports.

        If ``name`` ends with the globbing character (`*`), the generator iterates
        through all existing ports matching that pattern. Otherwise, only one port is
        returned. If it does not already exist, it is automatically created.

        Args:
            name (string): The matching pattern.

        Yields:
            (tupple): A tupple containing:

            * name (`string`): The full port name.
            * suffix (`string`): The part of the name matching the globbing character.
            * port (`Port`): The port object.

        """

        if name.endswith("*"):
            skip = len(name) - 1
            name = name[:-1]
            for key, port in self.ports.items():
                if key.startswith(name):
                    yield key, key[skip:], port
        else:
            yield name, "", getattr(self, name)

    def clear(self):
        """Reset all ports.

        It is assumed that numbered ports (i.e. those with a name ending with an
        underscore followed by numbers) are temporary and must be completely removed.
        The only exception is when they are bound to a default or named port.
        All other ports are simply emptied to avoid the cost of reinstanciating a new
        `Port` object before each update.

        """

        if not hasattr(self, "_re_dynamic_port"):
            self._re_dynamic_port = re.compile(".*_[0-9]+$")

        remove = []
        ids = []

        for name, port in self.ports.items():
            port.clear()
            if self._re_dynamic_port.match(name):
                if id(port) not in ids:
                    remove.append(name)
                    continue
            ids.append(id(port))

        for name in remove:
            del self.ports[name]
            delattr(self, name)

    @abstractmethod
    def update(self):
        """Update the input and output ports."""

        pass

    def terminate(self):
        """Perform cleanup upon termination."""

        pass
