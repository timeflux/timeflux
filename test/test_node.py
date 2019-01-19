"""Tests for node.py"""

import pytest
from types import GeneratorType
from timeflux.core.node import Node
from timeflux.core.io import Port

class DummyNode(Node):
    def update():
        pass

def test_create_ports():
    n = DummyNode()
    assert n.i == n.ports['i']
    assert n.i.data == None
    assert n.i_test.data == None
    n.o.set([[0,1],[2,3]], [0, 1])
    assert id(n.o.data) == id(n.ports['o'].data)

def test_clear_ports():
    n = DummyNode()
    n.o.set([[0,1],[2,3]], [0, 1])
    n.clear()
    assert n.o.data == None

def test_create_dynamic_port():
    n = DummyNode()
    assert type(n.i_foo) == Port

def test_iterate():
    n = DummyNode()
    n.o_p_0.meta = 'foo'
    n.o_p_1.meta = 'bar'
    n.o_p_2.meta = 'baz'
    expected = ['o_p_0', 'o_p_1', 'o_p_2']
    result = [port[0] for port in list(n.iterate('o_p_*'))]
    assert result == expected