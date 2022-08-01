"""Tests for worker.py"""

import sys
import logging
import pytest
from timeflux.core.worker import Worker

def test_invalid_module(caplog):
    graph = {'id': 'graph_id', 'rate': 1, 'nodes': [{'id': 'node_id', 'module': 'foobar', 'class': 'Foobar', 'params': {}}]}
    w = Worker(graph)._run()
    assert caplog.record_tuples == [('timeflux.core.worker', logging.ERROR, "Node 'node_id': no module named 'foobar'"),]

def test_invalid_class(caplog):
    graph = {'id': 'graph_id', 'rate': 1, 'nodes': [{'id': 'node_id', 'module': 'timeflux.nodes.random', 'class': 'Foobar', 'params': {}}]}
    w = Worker(graph)._run()
    assert caplog.record_tuples == [('timeflux.core.worker', logging.ERROR, "Node 'node_id': no class named 'Foobar' in module 'timeflux.nodes.random'"),]

def test_missing_param(caplog):
    graph = {'id': 'graph_id', 'rate': 1, 'nodes': [{'id': 'node_id', 'module': 'timeflux.nodes.lsl', 'class': 'Send', 'params': {}}]}
    w = Worker(graph)._run()
    if sys.version_info.minor >= 10:
        msg = "Node 'node_id': Send.__init__() missing 1 required positional argument: 'name'"
    else:
        msg = "Node 'node_id': __init__() missing 1 required positional argument: 'name'"
    assert caplog.record_tuples == [('timeflux.core.worker', logging.ERROR, msg),]
    assert True

def test_invalid_param(caplog):
    graph = {'id': 'graph_id', 'rate': 1, 'nodes': [{'id': 'node_id', 'module': 'timeflux.nodes.random', 'class': 'Random', 'params': {'foo': 'bar'}}]}
    w = Worker(graph)._run()
    if sys.version_info.minor >= 10:
        msg = "Node 'node_id': Random.__init__() got an unexpected keyword argument 'foo'"
    else:
        msg = "Node 'node_id': __init__() got an unexpected keyword argument 'foo'"
    assert caplog.record_tuples == [('timeflux.core.worker', logging.ERROR, msg),]