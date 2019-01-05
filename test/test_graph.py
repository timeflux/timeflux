"""Tests for graph.py"""

import pytest
from networkx.exception import NetworkXUnfeasible
from timeflux.core.graph import Graph


test_dag = {'nodes': [{'id': 'node_1', 'module': 'timeflux.nodes.random', 'class': 'Random', 'params': {'columns': 5, 'rows_min': 1, 'rows_max': 10, 'value_min': 0, 'value_max': 5}}, {'id': 'node_2', 'module': 'timeflux.nodes.arithmetic', 'class': 'Add', 'params': {'value': 1}}, {'id': 'node_3', 'module': 'timeflux.nodes.debug', 'class': 'Log'}], 'edges': [{'source': 'node_1', 'target': 'node_2'}, {'source': 'node_2', 'target': 'node_3'}]}
test_dag_complex = {'nodes': [{'id': 'node_1'}, {'id': 'node_2'}, {'id': 'node_3'}, {'id': 'node_4'}, {'id': 'node_5'}], 'edges': [{'source': 'node_1', 'target': 'node_2'}, {'source': 'node_2', 'target': 'node_3'}, {'source': 'node_4', 'target': 'node_3'}, {'source': 'node_2', 'target': 'node_5'}]}
test_cyclic = {'nodes': [{'id': 'node_1'}, {'id': 'node_2'}, {'id': 'node_3'}], 'edges': [{'source': 'node_1', 'target': 'node_2'}, {'source': 'node_2', 'target': 'node_3'}, {'source': 'node_3', 'target': 'node_1'}]}


def test_build():
    g = Graph(test_dag).build()
    assert g.number_of_nodes() == 3
    assert g.number_of_edges() == 2

def test_path_dag():
    p = Graph(test_dag).traverse()
    assert p == [{'node': 'node_1', 'predecessors': []}, {'node': 'node_2', 'predecessors': [{'node': 'node_1', 'src_port': 'o', 'dst_port': 'i', 'copy': True}]}, {'node': 'node_3', 'predecessors': [{'node': 'node_2', 'src_port': 'o', 'dst_port': 'i', 'copy': True}]}]

def test_path_dag_complex():
    p = Graph(test_dag_complex).traverse()
    assert p == [{'node': 'node_4', 'predecessors': []}, {'node': 'node_1', 'predecessors': []}, {'node': 'node_2', 'predecessors': [{'node': 'node_1', 'src_port': 'o', 'dst_port': 'i', 'copy': True}]}, {'node': 'node_5', 'predecessors': [{'node': 'node_2', 'src_port': 'o', 'dst_port': 'i', 'copy': True}]}, {'node': 'node_3', 'predecessors': [{'node': 'node_2', 'src_port': 'o', 'dst_port': 'i', 'copy': True}, {'node': 'node_4', 'src_port': 'o', 'dst_port': 'i', 'copy': True}]}]

def test_path_cyclic():
    with pytest.raises(NetworkXUnfeasible):
        Graph(test_cyclic).traverse()
