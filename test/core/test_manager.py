"""Tests for manager.py"""

import os
import json
import pytest
from timeflux.core.manager import Manager

test_config = {'graphs': [{'nodes': [{'id': 'node_1', 'module': 'timeflux.nodes.random', 'class': 'Random', 'params': {'columns': 5, 'rows_min': 1, 'rows_max': 10, 'value_min': 0, 'value_max': 5, 'seed': 1}}, {'id': 'node_2', 'module': 'timeflux_example.nodes.arithmetic', 'class': 'Add', 'params': {'value': 1}}, {'id': 'node_3', 'module': 'timeflux.nodes.debug', 'class': 'Display', 'params': {}}], 'edges': [{'source': 'node_1', 'target': 'node_2'}, {'source': 'node_2', 'target': 'node_3'}], 'id': None, 'rate': 1}]}


def test_load_invalid_config():
    with pytest.raises(ValueError):
        Manager(42)

def test_load_invalid_file():
    with pytest.raises(FileNotFoundError):
        Manager('foo.yaml')

def test_load_yaml_file():
    m = Manager(pytest.path + '/graphs/test.yaml')
    assert m._graphs == test_config['graphs']

def test_load_json_file():
    m = Manager(pytest.path + '/graphs/test.json')
    assert m._graphs == test_config['graphs']

def test_load_dict():
    m = Manager(test_config)
    assert m._graphs == test_config['graphs']

def test_template():
    os.environ['FOOBAR'] = 'MyClass'
    m = Manager(pytest.path + '/graphs/template.yaml')
    assert m._graphs[0]['nodes'][0]['class'] == 'MyClass'

def test_import():
    m = Manager(pytest.path + '/graphs/import.yaml')
    assert len(m._imports) == 4

def test_import_recursive():
    m = Manager(pytest.path + '/graphs/import3.yaml')
    assert len(m._imports) == 5

def test_validation_failure():
    with pytest.raises(ValueError):
        m = Manager({'graphs!': [{'nodes': [{'id': 'node_id', 'module': 'foobar', 'class': 'Foobar'}]}]})
    with pytest.raises(ValueError):
        m = Manager({'graphs': [{'nodes': [{'id': 'node_id', 'module': '.foobar', 'class': 'Foobar'}]}]})
    with pytest.raises(ValueError):
        m = Manager({'graphs': [{'nodes': [{'id': 'node_id', 'module': 'foobar.', 'class': 'Foobar'}]}]})
    with pytest.raises(ValueError):
        m = Manager({'graphs': [{'nodes': [{'id': 'node_id', 'module': 'foobar.', 'class': 'foobar'}]}]})
    with pytest.raises(ValueError):
        m = Manager({'graphs': [{'nodes': [{'id': 'node!', 'module': 'foobar.', 'class': 'foobar'}]}]})

def test_validation_success():
    m = Manager({'graphs': [{'nodes': [{'id': 'node_id', 'module': 'foobar', 'class': 'Foobar'}]}]})
    assert m != None
    m = Manager({'graphs': [{'nodes': [{'id': 'node_id', 'module': 'foobar', 'class': 'Foobar'}], 'edges': [{'source': 'node_id', 'target': 'node_id'}]}]})
    assert m != None
    m = Manager({'graphs': [{'nodes': [{'id': 'node_id', 'module': 'foobar', 'class': 'Foobar'}], 'edges': [{'source': 'node:*', 'target': 'node_id'}]}]})
    assert m != None
    m = Manager({'graphs': [{'nodes': [{'id': 'node_id', 'module': 'foobar', 'class': 'Foobar'}], 'edges': [{'source': 'node:1', 'target': 'node:abc'}]}]})
    assert m != None
