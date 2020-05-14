"""Branch base class."""

from timeflux.core.node import Node
from timeflux.core.worker import Worker
from timeflux.core.scheduler import Scheduler
from timeflux.core.validate import validate


class Branch(Node):
    def __init__(self, graph=None):
        self._scheduler = None
        if graph:
            self.load(graph)

    def update(self):
        self.run()

    def load(self, graph):
        """Initialize the graph.

        Args:
            graph (dict): The graph.

        """
        try:
            validate(graph, "graph")
        except:
            raise ValueError("Invalid branch")
        worker = Worker(graph)
        path, nodes = worker.load()
        self._scheduler = Scheduler(path, nodes, 0)

    def run(self):
        """Execute the graph once."""
        if self._scheduler:
            self._scheduler.next()

    def get_port(self, node_id, port_id="o"):
        """Get a port from the graph.

        Args:
            node_id (string): The node id.
            port_id (string): The port name. Default: `o`.

        Returns:
            Port: A reference to the requested port.

        """
        return getattr(self._scheduler._nodes[node_id], port_id)

    def set_port(self, node_id, port_id="i", data=None, meta=None, persistent=True):
        """Set a port's data and meta.

        Args:
            node_id (string): The node id.
            port_id (string): The port name. Default: `i`.
            data (DataFrame, DataArray): The data. Default: `None`.
            meta (dict): The meta. Default: `None`.
            persistent (boolean): Set the persistence of data and meta.
                                  If `True`, the port will not be cleared during graph execution. Default: `True`.

        """
        port = self.get_port(node_id, port_id)
        port.persistent = persistent
        if not meta:
            meta = {}
        port.data = data
        port.meta = meta
