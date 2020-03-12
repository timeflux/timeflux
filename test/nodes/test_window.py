"""Tests for window.py"""

import pandas as pd
import pytest

from timeflux.helpers import testing as helpers
from timeflux.nodes.window import Window


@pytest.fixture(scope="function")
def data():
    return helpers.DummyData(jitter=0)


def check_parameters():
    with pytest.raises(ValueError):
        node = Window(length=1, step=2)


def test_not_enough_data(data):
    node = Window(length=1, step=None)
    # Not enough data to
    node.i.data = data.next(5)
    node.update()
    assert node.o.data == None


def test_enough_data(data):
    # enough data to
    node = Window(length=1, step=None)
    node.i.data = data.next(11)
    node.update()
    pd.testing.assert_frame_equal(node.o.data, data._data.iloc[0:10, :])


def test_no_overlap(data):
    # reset generator and node states
    node = Window(length=1, step=None)
    # Send enough data for an epoch, but no event
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
    # step lower than length
    node = Window(length=1, step=.2)
    data.reset()
    not_monotonic = data.next(11)
    not_monotonic.index = not_monotonic.index[:5].append([not_monotonic.index[3:-2]])
    node.i.data = not_monotonic
    node.update()


def test_no_memory_leek(data):
    # step lower than length
    node = Window(length=1, step=.2)
    data.reset()
    node.i.data = data.next(30)
    node.update()
    assert len(node._buffer) == 8
