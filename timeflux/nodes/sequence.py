"""timeflux.nodes.sequence: generate a sequence"""

from timeflux.core.node import Node


class Sequence(Node):
    def __init__(self):
        """Generate a sequence"""
        self._current = 0

    def update(self):
        self.o.set([self._current])
        self._current += 1
