"""timeflux.core.graph: handle graphs"""

import networkx as nx
from timeflux.core.exceptions import GraphDuplicateNode, GraphUndefinedNode


class Graph:

    """Graph helpers"""

    def __init__(self, graph):
        """
        Initialize graph.

        Parameters
        ----------
        graph : dict
          The graph, which structure can be found in `test.yaml`. Must be an acyclic directed graph.
          Multiple edges between two nodes are allowed.

        See also
        --------
        build

        """

        self._graph = graph  # The original graph
        self.graph = None  # The multi directed graph
        self.path = None  # The sorted graph

    def build(self):
        """
        Build graph.

        Returns
        -------
        MultiDiGraph
            A graph object, suitable for computation.

        """

        self.graph = nx.MultiDiGraph()
        nodes = set()
        for node in self._graph["nodes"]:
            node = node.copy()
            id = node.pop("id")
            if id in nodes:
                raise GraphDuplicateNode(f"Duplicate node '{id}'")
            nodes.add(id)
            self.graph.add_node(id, **node)
        if "edges" in self._graph:
            sources = set()
            for edge in self._graph["edges"]:
                src = edge["source"].split(":")
                src_node = src[0]
                if src_node not in nodes:
                    raise GraphUndefinedNode(f"Undefined node '{src_node}'")
                src_port = "o_" + src[1] if len(src) > 1 else "o"
                src = src_node + ":" + src_port
                copy = src in sources
                sources.add(src)
                dst = edge["target"].split(":")
                dst_node = dst[0]
                if dst_node not in nodes:
                    raise GraphUndefinedNode(f"Undefined node '{dst_node}'")
                dst_port = "i_" + dst[1] if len(dst) > 1 else "i"
                self.graph.add_edge(
                    src_node, dst_node, src_port=src_port, dst_port=dst_port, copy=copy
                )
        return self.graph

    def traverse(self):
        """
        Sort nodes in a format suitable for traversal.

        Returns
        -------
        list of dicts:
            List of node ids and their predecessors.

        """

        if not self.graph:
            self.build()
        self.path = []
        for node in nx.algorithms.dag.topological_sort(self.graph):
            predecessors = []
            for predecessor in self.graph.predecessors(node):
                for _, edge in self.graph[predecessor][node].items():
                    predecessors.append(
                        {
                            "node": predecessor,
                            "src_port": edge["src_port"],
                            "dst_port": edge["dst_port"],
                            "copy": edge["copy"],
                        }
                    )
            self.path.append({"node": node, "predecessors": predecessors})
        return self.path
