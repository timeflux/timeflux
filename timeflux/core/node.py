"""timeflux.core.node: node base class"""

import logging
from abc import ABC, abstractmethod
from timeflux.core.io import Port

class Node(ABC):

    def __getattr__(self, name):
        """
        Create input and output ports on the fly.

        Parameters
        ----------
        name : string
            The name of the port, prefixed by 'i_' (input) or 'o_' (output).
            Default ports are also allowed ('i' or 'o').
        """

        if name == 'i' or name.startswith('i_') or name == 'o' or name.startswith('o_'):
            if not self.ports: self.ports = {}
            self.ports[name] = Port()
            setattr(self, name, self.ports[name])
            return self.ports[name]

    def clear(self):
        """Reset all ports."""
        if self.ports:
            for name, port in self.ports.items():
                port.data = None

    @abstractmethod
    def update(self):
        """Update the input and output ports."""
        pass