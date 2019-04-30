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
        graph['rate'] = 0
        worker = Worker(graph)
        path, nodes = worker.load()
        self._scheduler = Scheduler(path, nodes, 0)

    def run(self):
        self._scheduler.next()

    def get_port(self, node_id, port_id='o'):
        return getattr(self._scheduler._nodes[node_id], port_id)

    def set_port(self, node_id, port_id='i', data=None, meta=None):
        port = self.get_port(node_id, port_id)
        if data: port.data = data
        if meta: port.meta = meta
