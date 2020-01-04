"""Tests for branch.py"""

import pytest
import time
from timeflux.core.registry import Registry
from timeflux.core.branch import Branch

Registry.cycle_start = time.time()

def test_invalid():
    with pytest.raises(ValueError):
        Branch().load({})

def test_branch():
    graph = {
        'nodes': [
            {
                'id': 'node_1',
                'module': 'timeflux.nodes.random',
                'class': 'Random',
                'params': {
                    'seed': 1,
                    'rows_min': 1,
                    'rows_max': 1,
                    'columns': 1
                }
            },
            {
                'id': 'node_2',
                'module': 'timeflux_example.nodes.arithmetic',
                'class': 'Add',
                'params': {
                    'value': 1
                }
            },
            {
                'id': 'node_3',
                'module': 'timeflux_example.nodes.arithmetic',
                'class': 'Add',
                'params': {
                    'value': 1
                }
            },
            {
                'id': 'node_4',
                'module': 'timeflux_example.nodes.arithmetic',
                'class': 'Add',
                'params': {
                    'value': 1
                }
            },
            {
                'id': 'debug',
                'module': 'timeflux.nodes.debug',
                'class': 'Display',
                'params': {}
            }
        ],
        'edges': [
            {
                'source': 'node_1',
                'target': 'node_2'
            },
            {
                'source': 'node_2',
                'target': 'node_3'
            },
            {
                'source': 'node_1',
                'target': 'node_4'
            }
        ]
    }
    branch = Branch(graph)
    branch.run()
    assert branch.get_port('node_3').data[0][0] == 7
    assert branch.get_port('node_4').data[0][0] == 6
    assert id(branch.get_port('node_1', 'o').data) == id(branch.get_port('node_2', 'i').data)
    assert id(branch.get_port('node_2', 'i').data) == id(branch.get_port('node_2', 'o').data)
    assert id(branch.get_port('node_2', 'o').data) == id(branch.get_port('node_3', 'i').data)
    assert id(branch.get_port('node_3', 'i').data) == id(branch.get_port('node_3', 'o').data)
    assert id(branch.get_port('node_1', 'o').data) != id(branch.get_port('node_4', 'i').data)
    assert id(branch.get_port('node_4', 'i').data) == id(branch.get_port('node_4', 'o').data)

