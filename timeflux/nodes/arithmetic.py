"""timeflux.nodes.arithmetic: basic operations"""

from timeflux.core.node import Node
from timeflux.core.io import Port

class Add(Node):

    """
    Attributes:
        i (Port): default input
        o (Port): default output

    Example:
        .. literalinclude:: /../test/graphs/test.yaml
           :language: yaml
    """

    def __init__(self, value):
        self._value = value

    def update(self):
        self.o.data = self.i.data
        self.o.data += self._value


class MatrixAdd(Node):

    def __init__(self):
        self.i_m1 = Port()
        self.i_m2 = Port()

    def update(self):
        self.o.data = self.i_m1.data + self.i_m2.data