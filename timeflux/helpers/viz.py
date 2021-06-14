#!/usr/bin/env python

"""Export Timeflux apps as images.

This module can be imported or used as a standalone tool. It is quite useful to visually
inspect complex apps. It offers a graphical representation of multiple direcred acyclic
graphs. It uses Graphviz under the hood, and outputs the `dot` representation on the
standard output, so it can optionnally be redirected to a file for further processing.

Example:
    ``python -m timeflux.helpers.viz foobar.yaml``

"""

import sys
import os
from timeflux.core.manager import Manager
from timeflux.core.graph import Graph

try:
    import graphviz as gv
except ModuleNotFoundError:
    raise SystemExit(
        "Graphviz is not installed. Optional dependencies can be installed with: 'pip install timeflux[opt]'."
    )


def yaml_to_png(filename, format="png", sort=False):
    """Generate an image from a YAML application file.

    Args:
        filename (string): The path to the YAML application file.
        format (string): The image format. Default: `png`.
        sort (boolean): If `True`, the graphs will be sorted in the same topological order
                        that is used to run the application. Default: `False`.

    """

    # Load graphs
    graphs = Manager(filename)._graphs

    # Default graph attributes
    graph_attr = {
        "splines": "spline",
        "rankdir": "LR",
        "style": "filled",
        "fillcolor": "lightgrey",
        "color": "black",
        "fontname": "helvetica",
        "fontsize": "16",
    }

    # Default node attributes
    node_attr = {
        "shape": "ellipse",
        "style": "filled",
        "fillcolor": "white",
        "color": "black",
        "fontname": "helvetica",
        "fontsize": "14",
    }

    # Default edge attributes
    edge_attr = {
        "fontname": "helvetica",
        "fontsize": "9",
        "fontcolor": "snow4",
        "labeldistance": "1.5",
        "labelangle": "-25",
    }

    # Special nodes
    broker = None
    publishers = []
    subscribers = []

    # Initialize the viz using the default dot engine
    dot = gv.Digraph(
        format=format, graph_attr=graph_attr, node_attr=node_attr, edge_attr=edge_attr
    )

    # Add clusters
    for ci, graph in enumerate(graphs):
        cluster_name = f"cluster_{ci}"
        with dot.subgraph(name=cluster_name) as cluster:
            # Set label
            if "id" in graph:
                cluster.attr(label=graph["id"])
            # Add nodes
            nodes = {}
            for ni, node in enumerate(graph["nodes"]):
                node_name = f"{cluster_name}_node_{ni}"
                nodes[node["id"]] = node_name
                cluster.node(node_name, label=node["id"])
                # Check for special nodes
                if node["module"] == "timeflux.nodes.zmq":
                    if node["class"] in ("Broker", "BrokerMonitored", "BrokerLVC"):
                        broker = node_name
                    elif node["class"] == "Pub":
                        publishers.append(
                            {"name": node_name, "topic": node["params"]["topic"]}
                        )
                    elif node["class"] == "Sub":
                        subscribers.append(
                            {"name": node_name, "topics": node["params"]["topics"]}
                        )

            # Add edges
            if "edges" in graph:
                for edge in graph["edges"]:
                    src = edge["source"].split(":")
                    dst = edge["target"].split(":")
                    taillabel = src[1] if len(src) == 2 else ""
                    headlabel = dst[1] if len(dst) == 2 else ""
                    cluster.edge(
                        nodes[src[0]],
                        nodes[dst[0]],
                        taillabel=taillabel,
                        headlabel=headlabel,
                    )

            # Sort nodes according to the topoligical order of the graph
            # This is still a bit hacky and should be considered as a WIP.
            if sort:
                path = Graph(graph).traverse()
                edge = []
                for node in path:
                    edge.append(nodes[node["node"]])
                    if len(edge) == 2:
                        cluster.edge(edge[0], edge[1], style="invis", weight="100")
                        edge = [edge[1]]

    # Add special edges
    if broker:
        dot.attr("edge", style="dashed")
        for publisher in publishers:
            dot.edge(publisher["name"], broker, label=publisher["topic"])
        for subscriber in subscribers:
            for topic in subscriber["topics"]:
                dot.edge(broker, subscriber["name"], label=topic)

    print(dot.source)
    out = os.path.splitext(os.path.basename(filename))[0]
    dot.render(out, cleanup=True, view=True)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit()
    yaml_to_png(sys.argv[1], sort=False)
