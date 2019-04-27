"""timeflux.core.worker: spawn processes."""

import importlib
import logging
from multiprocessing import Process
from timeflux.core.graph import Graph
from timeflux.core.scheduler import Scheduler
from timeflux.core.registry import Registry
from timeflux.core.exceptions import WorkerLoadError

class Worker:

    """Spawn a process and launch a scheduler."""

    def __init__(self, graph):
        self._graph = graph

    def run(self):
        """Run the process"""
        p = Process(target=self._run, args=(logging.getLogger().getEffectiveLevel(),), name=self._graph['id'])
        p.start()
        return p.pid

    def load(self):

        # Build the graph and compute the traversal path
        g = Graph(self._graph)
        graph = g.build()
        path = g.traverse()

        # Set rate
        Registry.rate = self._graph['rate']

        # Load nodes
        nodes = {}
        for step in path:
            node = self._load_node(graph.nodes[step['node']], step['node'])
            nodes[step['node']] = node

        return path, nodes

    def _run(self, logging_level='INFO'):

        # Set the root logging level, which is is not propagated on Windows
        logging.getLogger().setLevel(logging_level)

        # Initialize the graph and instantiate the nodes
        try:
            path, nodes = self.load()
        except KeyboardInterrupt as error:
            logging.debug('Interrupting')
            return False
        except Exception as error:
            logging.error(error)
            return False

        # Launch scheduler and run it
        scheduler = Scheduler(path, nodes, self._graph['rate'])
        scheduler.run()

    def _load_node(self, node, nid):
        """Import a module and instantiate class."""

        # Import module
        try:
            m = importlib.import_module(node['module'])
        except ModuleNotFoundError:
            raise WorkerLoadError(f"Node '{nid}': no module named '{node['module']}'")

        # Get class
        try:
            c = getattr(m, node['class'])
        except AttributeError:
            raise WorkerLoadError(f"Node '{nid}': no class named '{node['class']}' in module '{node['module']}'")

        # Instantiate class
        try:
            n = c(**node['params'])
        except TypeError as error:
            raise WorkerLoadError(f"Node '{nid}': {error}")

        return n
