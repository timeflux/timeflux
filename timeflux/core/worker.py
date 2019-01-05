"""timeflux.core.worker: spawn processes."""

import importlib
from multiprocessing import Process
from timeflux.core.graph import Graph
from timeflux.core.scheduler import Scheduler
from timeflux.core.registry import Registry

class Worker:

    """Spawn a process and launch a scheduler."""

    def __init__(self, graph):
        self._graph = graph

         # TODO: set defaults during validation
        if not 'id' in self._graph: self._graph['id'] = None

    def run(self):
        """Run the process"""
        p = Process(target=self._run, name=self._graph['id'])
        p.start()
        return p.pid

    def _run(self):

        # Build the graph and compute the traversal path
        g = Graph(self._graph)
        graph = g.build()
        path = g.traverse()

        # TODO: set defaults during validation
        if not 'rate' in self._graph: self._graph['rate'] = 1
        Registry.rate = self._graph['rate']

        # Load nodes
        nodes = {}
        for step in path:
            node = self._load_node(graph.nodes[step['node']])
            nodes[step['node']] = node

        # Launch scheduler and run it
        scheduler = Scheduler(path, nodes, self._graph['rate'])
        scheduler.run()

    def _load_node(self, node):
        """Import a module and instantiate class."""
        m = importlib.import_module(node['module'])
        c = getattr(m, node['class'])

        # TODO: set defaults during validation
        if not 'params' in node: node['params'] = {}

        i = c(**node['params'])

        return i
