"""Tests for window.py"""

import pandas as pd

from timeflux.helpers import testing as helpers
from timeflux.nodes.window import Window

data = helpers.DummyData(jitter=0)

rate = 10
length = 1
step = None
node = Window(length=length, step=None)


def test_not_enough_data():
    # Not enough data to
    node.i.data = data.next(5)
    node.update()
    assert node.o.data == None


def test_enough_data():
    # Not enough data to
    node.i.data = data.next(6)
    node.update()
    pd.testing.assert_frame_equal(node.o.data, data._data.iloc[1:11, :])


def test_no_overlap():
    # reset generator and node states
    data.reset()
    node.__init__(length=length, step=None)
    node.clear()
    # Send enough data for an epoch, but no event
    node.i.data = data.next(11)
    node.update()
    o1 = node.o.data.copy()
    node.i.data = data.next(10)
    node.update()
    o2 = node.o.data.copy()
    pd.testing.assert_frame_equal(o1.append(o2), data._data.iloc[1:21])


def test_low_step():
    # step lower than length
    step = .2
    node = Window(length=length, step=step)
    data.reset()
    node.i.data = data.next(11)
    node.update()
    o1 = node.o.data
    node.i.data = data.next(2)
    node.update()
    o2 = node.o.data
    assert (o2.index[0] - o1.index[0]).total_seconds() == step
