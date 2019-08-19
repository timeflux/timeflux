"""Tests for gate.py"""
import numpy as np
import pandas as pd
import xarray as xr
from timeflux.helpers.testing import DummyData, DummyXArray
from timeflux.nodes.gate import Gate

xarray_data = DummyXArray()
pandas_data = DummyData()


node = Gate(event_opens='foo_begins', event_closes='foo_ends', truncate=True)



def test_gate_silent():
    pandas_data.reset()
    # Send data but no event
    node.i.data = pandas_data.next(20)
    node.update()

    assert node._status == 'closed'
    assert node.o.data == None


def test_send_opening_closing_event_in_separate_chunks():
    pandas_data.reset()
    # Send an opening event
    node.clear()
    node.i.data = pandas_data.next(5)
    time_open = pd.Timestamp('2018-01-01 00:00:00.104507143')  # Sync event to second sample
    event = pd.DataFrame([['foo_begins']], [time_open], columns=['label'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    expected_data = pd.DataFrame(
        [
            [0.658783, 0.692277, 0.849196, 0.249668, 0.489425],
            [0.221209, 0.987668, 0.944059, 0.039427, 0.705575],
            [0.925248, 0.180575, 0.567945, 0.915488, 0.033946],
            [0.69742, 0.297349, 0.924396, 0.971058, 0.944266],
        ],
        [
            pd.Timestamp('2018-01-01 00:00:00.104507143'),
            pd.Timestamp('2018-01-01 00:00:00.202319939'),
            pd.Timestamp('2018-01-01 00:00:00.300986584'),
            pd.Timestamp('2018-01-01 00:00:00.396560186')

        ]
    )

    expected_meta = {'gate_status': 'open'}

    assert node._status == 'open'
    assert node._trigger == 'foo_ends'
    assert node.o.meta == expected_meta
    pd.testing.assert_frame_equal(node.o.data, expected_data)

    # Send a closing event
    node.clear()
    node.i.data = pandas_data.next(5)
    time_close = pd.Timestamp('2018-01-01 00:00:00.595580836')  # Sync event to second sample
    event = pd.DataFrame([['foo_ends']], [time_close], columns=['label'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    expected_data = pd.DataFrame(
        [
            [0.474214, 0.862043, 0.844549, 0.3191, 0.828915],
            [0.037008, 0.59627, 0.230009, 0.120567, 0.076953]
        ],
        [
            pd.Timestamp('2018-01-01 00:00:00.496559945'),
            pd.Timestamp('2018-01-01 00:00:00.595580836')

        ]
    )
    expected_meta = {'gate_status': 'closed',
                     'gate_times': [time_open, time_close]}


    assert node._status == 'closed'
    assert node._trigger == 'foo_begins'
    assert node.o.meta == expected_meta
    pd.testing.assert_frame_equal(node.o.data, expected_data)


def test_send_opening_closing_event_in_same_chunk():
    # Send an opening event and a closing event in the same chunk
    pandas_data.reset()
    node.clear()
    node.i.data = pandas_data.next(5)
    time_open = pd.Timestamp('2018-01-01 00:00:00.1')
    time_close = pd.Timestamp('2018-01-01 00:00:00.3')
    event = pd.DataFrame([['foo_begins'], ['foo_ends']], [time_open, time_close],
                         columns=['label'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    expected_data = pd.DataFrame(
        [
            [0.658783, 0.692277, 0.849196, 0.249668, 0.489425],
            [0.221209, 0.987668, 0.944059, 0.039427, 0.705575]
        ],
        [
            pd.Timestamp('2018-01-01 00:00:00.104507143'),
            pd.Timestamp('2018-01-01 00:00:00.202319939')
        ]
    )
    expected_meta = {'gate_status': 'closed',
                     'gate_times': [time_open, time_close]}

    assert node.o.meta == expected_meta
    pd.testing.assert_frame_equal(node.o.data, expected_data)


    assert node._status == 'closed'
    assert node._trigger == 'foo_begins'


def test_xarray_data():
    # Send an opening event and a closing event in the same chunk with XArray data
    xarray_data.reset()
    node.clear()
    node.i.data = xarray_data.next(5)
    time_open = pd.Timestamp('2018-01-01 00:00:00.1')
    time_close = pd.Timestamp('2018-01-01 00:00:00.3')
    event = pd.DataFrame([['foo_begins'], ['foo_ends']], [time_open, time_close],
                         columns=['label'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    expected_data = xr.DataArray(data=
    [
        [0.658783, 0.692277, 0.849196, 0.249668, 0.489425],
        [0.221209, 0.987668, 0.944059, 0.039427, 0.705575]
    ],
        coords=[
            [
                pd.Timestamp('2018-01-01 00:00:00.104507143'),
                pd.Timestamp('2018-01-01 00:00:00.202319939')
            ],
            np.arange(5)],
        dims=['time', 'space'])

    expected_meta = {'gate_status': 'closed',
                     'gate_times': [time_open, time_close]}

    assert node.o.meta == expected_meta
    xr.testing.assert_equal(node.o.data, expected_data)

    assert node._status == 'closed'
    assert node._trigger == 'foo_begins'
