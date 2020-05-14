"""Monitor a signal"""

from timeflux.core.node import Node


class Monitor(Node):
    def update(self):
        status = 0.0
        if self.i.data is not None:
            if not self.i.data.empty:
                status = 1.0
        self.o.set([status], names=["streaming"])
