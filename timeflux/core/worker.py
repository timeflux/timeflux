"""timeflux.core.worker: spawn processes."""

import importlib
import logging
import signal
from multiprocessing import Process
from timeflux.core.logging import get_queue, init_worker
from timeflux.core.graph import Graph
from timeflux.core.scheduler import Scheduler
from timeflux.core.registry import Registry
from timeflux.core.exceptions import *


class Worker:

    """Spawn a process and launch a scheduler."""

    def __init__(self, graph):
        self._graph = graph

    def run(self):
        """Run the process"""
        p = Process(target=self._run, args=(get_queue(),), name=self._graph["id"])
        p.start()
        return p

    def load(self):

        # Build the graph and compute the traversal path
        g = Graph(self._graph)
        graph = g.build()
        path = g.traverse()

        # Set rate
        Registry.rate = self._graph["rate"]

        # Load nodes
        nodes = {}
        for step in path:
            node = self._load_node(graph.nodes[step["node"]], step["node"])
            nodes[step["node"]] = node

        return path, nodes

    def _run(self, log_queue=None):

        # Initialize logging
        if log_queue:
            init_worker(log_queue)
        logger = logging.getLogger(__name__)

        scheduler = None

        try:
            # Initialize the graph and instantiate the nodes
            path, nodes = self.load()
            # Launch scheduler and run it
            scheduler = Scheduler(path, nodes, self._graph["rate"])
            scheduler.run()
        except KeyboardInterrupt:
            # Ignore further interrupts
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            logger.debug("Interrupting")
        except (
            GraphDuplicateNode,
            GraphUndefinedNode,
            WorkerLoadError,
            ValidationError,
        ) as error:
            logger.error(error)
        except WorkerInterrupt as error:
            logger.debug(error)
        except Exception as error:
            logger.exception(error)

        if scheduler is not None:
            logger.info("Terminating")
            scheduler.terminate()

    def _load_node(self, node, nid):
        """Import a module and instantiate class."""

        # Import module
        try:
            m = importlib.import_module(node["module"])
        except ModuleNotFoundError as error:
            if node["module"] in error.msg:
                # Missing or invalid node
                raise WorkerLoadError(
                    f"Node '{nid}': no module named '{node['module']}'"
                )
            else:
                # Missing or invalid dependency
                raise error

        # Get class
        try:
            c = getattr(m, node["class"])
        except AttributeError:
            raise WorkerLoadError(
                f"Node '{nid}': no class named '{node['class']}' in module '{node['module']}'"
            )

        # Instantiate class
        try:
            n = c(**node["params"])
        except TypeError as error:
            raise WorkerLoadError(f"Node '{nid}': {error}")

        return n
