"""Branch base class."""

from timeflux.core.node import Node
from timeflux.core.worker import Worker
from timeflux.core.scheduler import Scheduler

class Branch(Node):

    def __init__(self, graph=None):
        if graph:
            self.load(graph)

    def update(self):
        self.run()

    def load(self, graph):
        """Initialize the graph.

        Args:
            graph (dict): The graph.

        """
        graph['rate'] = 0
        worker = Worker(graph)
        path, nodes = worker.load()
        self._scheduler = Scheduler(path, nodes, 0)

    def run(self):
        """Execute the graph once."""
        self._scheduler.next()

    def get_port(self, node_id, port_id='o'):
        """Get a port from the graph.

        Args:
            node_id (string): The node id.
            port_id (string): The port name. Default: `o`.

        Returns:
            Port: A reference to the requested port.

        """
        return getattr(self._scheduler._nodes[node_id], port_id)

    def set_port(self, node_id, port_id='i', data=None, meta=None, persistant=True):
        """Set a port's data and meta.

        Args:
            node_id (string): The node id.
            port_id (string): The port name. Default: `i`.
            data (DataFrame, DataArray): The data. Default: `None`.
            meta (dict): The meta. Default: `None`.
            persistant (boolean): Set the persistance of data and meta.
                                  If `True`, the port will not be cleared during graph execution. Default: `True`.

        """
        port = self.get_port(node_id, port_id)
        port.persistant = persistant
        if not meta: meta = {}
        port.data = data
        port.meta = meta
