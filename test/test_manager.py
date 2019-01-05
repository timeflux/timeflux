"""Tests for manager.py"""

import json
import pytest
from timeflux.core.manager import Manager

test_config = {'graphs': [{'nodes': [{'id': 'node_1', 'module': 'timeflux.nodes.random', 'class': 'Random', 'params': {'columns': 5, 'rows_min': 1, 'rows_max': 10, 'value_min': 0, 'value_max': 5, 'seed': 1}}, {'id': 'node_2', 'module': 'timeflux_example.nodes.arithmetic', 'class': 'Add', 'params': {'value': 1}}, {'id': 'node_3', 'module': 'timeflux.nodes.debug', 'class': 'Display'}], 'edges': [{'source': 'node_1', 'target': 'node_2'}, {'source': 'node_2', 'target': 'node_3'}]}]}


def test_load_invalid_config():
    with pytest.raises(ValueError):
        Manager(42)

def test_load_invalid_file():
    with pytest.raises(FileNotFoundError):
        Manager('foo.yaml')

def test_load_yaml_file():
    m = Manager(pytest.path + '/graphs/test.yaml')
    assert m.config == test_config

def test_load_json_file():
    m = Manager(pytest.path + '/graphs/test.json')
    assert m.config == test_config

def test_load_json_string():
    m = Manager(json.dumps(test_config))
    assert m.config == test_config

def test_load_invalid_string():
    with pytest.raises(json.decoder.JSONDecodeError):
        Manager('{ "foo"')

def test_load_dict():
    m = Manager(test_config)
    assert m.config == test_config

# def test_run():
#    m = Manager(pytest.path + '/zmq.yaml')
#    assert m.run() == False

# def test_sub(pipe):
#     m = Manager(pytest.path + '/test2.yaml')
#     m.run()

