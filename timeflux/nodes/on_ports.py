"""timeflux.nodes.on_ports: Handle operations over ports """

import pandas as pd

from timeflux.core.node import Node


class Concat(Node):
    """ Concat list of data ports.
    """

    def __init__(self, axis=1, **kwargs):
        self._axis = axis
        self._kwargs = kwargs

    def update(self):
        ports = list(self.iterate("i*"))
        i_data = [port.data for (name, _, port) in ports if port.data is not None]
        if i_data:
            self.o.data = pd.concat(i_data, axis=self._axis, **self._kwargs)
