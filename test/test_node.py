"""Tests for node.py"""

import pytest
from timeflux.core.node import Node

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
