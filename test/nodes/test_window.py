"""Tests for window.py"""

import pandas as pd
import pytest

from timeflux.helpers import testing as helpers
from timeflux.nodes.window import Window, TimeWindow, SampleWindow


@pytest.fixture(scope="function")
def data():
    return helpers.DummyData(jitter=0)

def test_check_parameters():
    # index must be 'time' or 'sample'
    with pytest.raises(ValueError):
        node = Window(length=1, index='foo')
    # step should be lower than window length
    with pytest.raises(ValueError):
        node = Window(length=1, step=2)


def test_not_enough_data(data):
    # not enough data to window
    node = Window(length=1, step=None)
    node.i.data = data.next(5)
    node.update()
    assert node.o.data == None


def test_enough_data(data):
    # receive enough data window
    node = Window(length=1, step=None)
    node.i.data = data.next(11)
    node.update()
    pd.testing.assert_frame_equal(node.o.data, data._data.iloc[0:10, :])


def test_no_overlap(data):
    # test step equals length
    node = Window(length=1, step=None)
    node.i.data = data.next(11)
    node.update()
    o1 = node.o.data
    node.clear()
    node.i.data = data.next(11)
    node.update()
    o2 = node.o.data
    # assert continuity (no repeated samples)
    pd.testing.assert_frame_equal(o1.append(o2), data._data.iloc[0:20])
    # assert window length
    assert len(o2) == 10


def test_low_step(data):
    # step lower than length
    node = Window(length=1, step=.2)
    data.reset()
    node.i.data = data.next(11)
    node.update()
    o1 = node.o.data
    node.clear()
    node.i.data = data.next(3)
    node.update()
    o2 = node.o.data
    # assert step size
    assert (o2.index[0] - o1.index[0]).total_seconds() == 0.2
    # assert window length
    assert len(o2) == 10


def test_not_monotonic(data):
    # receive data that is not monotonic
    node = Window(length=1, step=.2)
    data.reset()
    not_monotonic = data.next(11)
    not_monotonic.index = not_monotonic.index[:5].append([not_monotonic.index[3:-2]])
    node.i.data = not_monotonic
    node.update()


def test_no_memory_leek(data):
    # receive too much data, avoid memory leak by truncating buffer
    node = Window(length=1, step=.2)
    data.reset()
    node.i.data = data.next(30)
    node.update()
    assert len(node._buffer) == 8
