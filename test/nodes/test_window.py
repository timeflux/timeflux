"""Tests for window.py"""

import pytest
import logging
import pandas as pd
from timeflux.helpers import testing as helpers
from timeflux.nodes.window import Window, TimeWindow, SampleWindow, Slide
from timeflux.core.exceptions import WorkerInterrupt


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
    pd.testing.assert_frame_equal(pd.concat([o1, o2]), data._data.iloc[0:20])
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

def test_no_memory_leak(data):
    # receive too much data, avoid memory leak by truncating buffer
    node = Window(length=1, step=.2)
    data.reset()
    node.i.data = data.next(30)
    node.update()
    assert len(node._buffer) == 8

def test_slide_missing_rate(data, caplog):
    node = Slide(length=1, step=.2)
    data.reset()
    node.i.data = data.next(10)
    with pytest.raises(WorkerInterrupt):
        node.update()
    assert caplog.record_tuples == [('timeflux.timeflux.nodes.window.Slide', 40, 'Rate is not specified')]

def test_slide_samples(data):
    node = Slide(length=.8, step=.1, rate=100)
    data.reset()
    node.i.data = data.next(10)
    node.update()
    assert node._rate == 100
    assert node._length_samples == 80
    assert node._step_samples == 10

def test_slide_new(data):
    node = Slide(length=.8, step=.1, rate=100)
    data.reset()
    node.i.data = data.next(22)
    node.update()
    assert node._index == -2
    assert len(node._windows) == 3
    assert len(node._windows[0]) == 22
    assert len(node._windows[1]) == 12
    assert len(node._windows[2]) == 2
    node.i.data = data.next(19)
    node.update()
    assert node._index == -1
    assert len(node._windows) == 5
    node.i.data = data.next(9)
    node.update()
    assert len(node._windows) == 5
    assert node._index == -10
    node.i.data = data.next(10)
    node.update()
    assert len(node._windows) == 6

def test_slide_append(data):
    node = Slide(length=.8, step=.1, rate=100)
    data.reset()
    node.i.data = data.next(22)
    node.update()
    assert len(node._windows) == 3
    assert len(node._windows[0]) == 22
    assert len(node._windows[1]) == 12
    assert len(node._windows[2]) == 2
    node.i.data = data.next(10)
    node.update()
    assert len(node._windows) == 4
    assert len(node._windows[0]) == 22 + 10
    assert len(node._windows[1]) == 12 + 10
    assert len(node._windows[2]) == 2 + 10
    assert len(node._windows[3]) == 2

def test_slide_send(data):
    node = Slide(length=1, step=.1, rate=100)
    data.reset()
    node.i.data = data.next(220)
    node.update()
    assert len(node.o_0.data) == 100
    assert len(node.o_12.data) == 100
    assert len(node._windows) == 9
    assert len(node._windows[0]) == 90
