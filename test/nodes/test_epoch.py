"""Tests for epoch.py"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from timeflux.core.branch import Branch
from timeflux.core.exceptions import WorkerInterrupt
from timeflux.helpers import testing as helpers
from timeflux.nodes.epoch import Epoch, Samples

def test_samples_no_rate():
    # Trigger an error if not rate can be found
    node = Samples(trigger='test', length=0.6, buffer=5)
    node.i.data = helpers.DummyData().next(20)
    with pytest.raises(WorkerInterrupt):
        node.update()

def test_samples_rate_from_meta():
    # Get rate from metadata
    node = Samples(trigger='test', length=0.6, buffer=5)
    node.i.data = helpers.DummyData().next(20)
    node.i.meta = { "rate": 250 }
    node.update()
    assert node._rate == 250

def test_samples_buffer():
    # Make sure that the circular buffer always keeps a little bit of past data
    node = Samples(trigger='test', length=0.6, buffer=3, rate=10)
    data = helpers.DummyData(rate=10)
    node.i.data = data.next(20)
    node.update()
    assert len(node._buffer) == 20
    node.i.data = data.next(20)
    node.update()
    length = round((node._buffer.index[-1] - node._buffer.index[0]).total_seconds())
    assert length == 3
    assert len(node._buffer) == 30
    node.i.data = data.next(20)
    node.update()
    length = round((node._buffer.index[-1] - node._buffer.index[0]).total_seconds())
    assert length == 3
    assert len(node._buffer) == 30

def test_samples_no_epoch():
    # Send enough data for an epoch, but no event
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    node.i.data = helpers.DummyData().next(20)
    node.update()
    assert node.o.data == None

def test_samples_invalid_event():
    # Make sure no epoch is fetched on an invalid trigger
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    node.i.data = helpers.DummyData().next(20)
    time = node.i.data.index[1]  # Sync event to second sample
    event = pd.DataFrame([['invalid', 'foobar']], [time], columns=['label', 'data'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    assert node.o.data == None

def test_samples_outdated_event(caplog):
    # Send an event trigger in the past
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    node.i.data = helpers.DummyData().next(5)
    time = pd.Timestamp('2010-01-01 00:00:00.000000000')
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    assert node.o.data == None
    assert caplog.record_tuples[0][2].startswith('Oudated event')

def test_samples_send_event_short_data():
    # Send an event trigger, but not enough data to capture the epoch in one round
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    node.i.data = helpers.DummyData().next(5)
    time = node.i.data.index[0]  # Sync event to first sample
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    assert node.o.data == None

def test_samples_receive_epoch():
    # Send enough data to complete one epoch in one one round
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    node.i.data = helpers.DummyData().next(20)
    time = node.i.data.index[1]  # Sync event to second sample
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    expected_meta = {
        'rate': 10,
        'epoch': {
            'onset': pd.Timestamp('2018-01-01 00:00:00.104507143'),
            'context': 'foobar'
        }
    }
    assert node.o == node.o_0
    assert node.o.meta == expected_meta
    assert node.o.data.index[0] == expected_meta["epoch"]["onset"]
    assert len(node.o.data) == 6

def test_samples_append_epoch():
    # Complete one epoch in two passes
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    data = helpers.DummyData()
    node.i.data = data.next(5)
    time = node.i.data.index[1]  # Sync event to second sample
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    assert len(node._epochs[0]["data"]) == 4
    assert node.o.data == None
    node.clear()
    node.i.data = data.next(5)
    node.update()
    expected_meta = {
        'rate': 10,
        'epoch': {
            'onset': pd.Timestamp('2018-01-01 00:00:00.104507143'),
            'context': 'foobar'
        }
    }
    assert node.o.meta == expected_meta
    assert node.o.data.index[0] == expected_meta["epoch"]["onset"]
    assert len(node.o.data) == 6

def test_samples_non_monotonic_buffer():
    # Make sure that the buffer does not error on non-monotonic data
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    node.i.data = helpers.DummyData().next()
    node.update()
    node.i.data = helpers.DummyData().next()
    node.update()

def test_samples_non_monotonic_epoch():
    # Handle epoching on non-monotonic data
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    data = helpers.DummyData()
    data_past = data.next(5)
    data_present = data.next(5)
    data_future = data.next(5)
    node.i.data = data_present
    time = data_present.index[1]
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])
    node.i_events.data = event
    node.update()
    assert len(node._epochs[0]["data"]) == 4
    assert node.o.data == None
    node.clear()
    node.i.data = data_past
    node.update()
    assert len(node._epochs[0]["data"]) == 4
    node.clear()
    node.i.data = data_future
    node.update()
    expected_meta = {
        'rate': 10,
        'epoch': {
            'onset': pd.Timestamp('2018-01-01 00:00:00.595580836'),
            'context': 'foobar'
        }
    }
    assert node.o.meta == expected_meta
    assert node.o.data.index[0] == expected_meta["epoch"]["onset"]
    assert len(node.o.data) == 6

def test_samples_future_event():
    # Handle future events
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    node.i.data = helpers.DummyData().next(10)
    time = pd.Timestamp('2042-01-01')
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])
    node.i_events.data = event
    node.update()
    assert len(node._epochs) == 1
    assert node._epochs[0]["data"] == None
    assert node.o.data == None

def test_samples_multiple_events():
    # Make sure the correct onset is caught
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    node.i.data = helpers.DummyData().next(10)
    event = pd.DataFrame(
        [
            ['invalid_1', 'foo'],
            ['test', 'bar'],
            ['invalid_2', 'baz']
        ], [
            node.i.data.index[0],
            node.i.data.index[1],
            node.i.data.index[2]
        ], columns=['label', 'data'])
    node.i_events.data = event
    node.update()
    assert node.o.meta['epoch']['onset'] == node.i.data.index[1]

def test_samples_multiple_triggers():
    # Mutliple epochs should be returned
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    data = helpers.DummyData()
    data_1 = data.next(10)
    data_2 = data.next(3)
    data_3 = data.next(10)
    event = pd.DataFrame(
        [
            ['test', 'foo'],
            ['test', 'bar'],
            ['test', 'baz']
        ], [
            data_1.index[0],
            data_1.index[8],
            data_2.index[0]
        ], columns=['label', 'data'])
    node.i_events.data = event
    node.i.data = data_1
    node.update()
    assert len(node._epochs) == 2
    assert node.o.meta['epoch']['context'] == 'foo'
    assert len(node._epochs[0]['data']) == 2
    assert node._epochs[1]['data'] == None
    node.clear()
    node.i.data = data_2
    node.update()
    assert len(node._epochs) == 2
    assert len(node._epochs[0]['data']) == 5
    assert len(node._epochs[1]['data']) == 3
    node.clear()
    node.i.data = data_3
    node.update()
    assert len(node._epochs) == 0
    assert node.o_0.meta['epoch']['context'] == 'bar'
    assert node.o_1.meta['epoch']['context'] == 'baz'

def test_samples_jumbled_events():
    # Send correct epochs
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    data = helpers.DummyData()
    data_1 = data.next(10)
    data_2 = data.next(10)
    event = pd.DataFrame(
        [
            ['test', 'foo'],
            ['test', 'bar'],
        ], [
            data_2.index[0],
            data_1.index[0]
        ], columns=['label', 'data'])
    node.i_events.data = event
    node.i.data = data_1
    node.update()
    assert node.o.meta['epoch']['context'] == 'bar'
    node.clear()
    node.i.data = data_2
    node.update()
    assert node.o.meta['epoch']['context'] == 'foo'

def test_samples_unsynced_event():
    # Correctly triggers on the immediately following timestamp
    node = Samples(trigger='test', length=0.6, buffer=5, rate=10)
    node.i.data = helpers.DummyData().next(10)
    time = pd.Timestamp('2018-01-01')
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])
    node.i_events.data = event
    node.update()
    assert node.o.meta['epoch']['onset'] == time
    assert node.o.data.index[0] == node.i.data.index[1]

def test_samples_offset_positive():
    # Apply a positive offset to the signal
    node = Samples(trigger='test', length=0.3, buffer=5, rate=10, offset=0.2)
    node.i.data = helpers.DummyData().next(10)
    time = pd.Timestamp('2018-01-01')
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])
    node.i_events.data = event
    node.update()
    assert node.o.meta['epoch']['onset'] == time
    assert node.o.data.index[0] == node.i.data.index[2]

def test_samples_offset_negative():
    # Apply a negative offset to the signal
    node = Samples(trigger='test', length=0.3, buffer=5, rate=10, offset=-0.2)
    node.i.data = helpers.DummyData().next(10)
    time = pd.Timestamp('2018-01-01 00:00:00.5')
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])
    node.i_events.data = event
    node.update()
    assert node.o.meta['epoch']['onset'] == time
    assert node.o.data.index[0] == node.i.data.index[3]


data = helpers.DummyData()

rate = 10
before = .2
after = .6
node = Epoch(event_trigger='test', before=before, after=after)


def test_no_epoch():
    # Send enough data for an epoch, but no event
    node.i.data = data.next(20)
    node.update()
    assert node.o.data == None


def test_send_event_short_data():
    # Send an event trigger, but not enough data to capture the epoch in one round
    node.clear()
    node.i.data = data.next(5)
    time = node.i.data.index[1]  # Sync event to second sample
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    assert node.o.data == None


def test_receive_epoch():
    # Send enough data to complete an epoch
    node.clear()
    node.i.data = data.next()
    node.update()
    expected_meta = {
        'epoch': {
            'onset': pd.Timestamp('2018-01-01 00:00:02.096394939'),
            'context': 'foobar',
            'before': before,
            'after': after
        }
    }
    expected_data = pd.DataFrame(
        [
            [0.176528, 0.220486, 0.186438, 0.779584, 0.350125],
            [0.057843, 0.969103, 0.883786, 0.927752, 0.994908],
            [0.173895, 0.396242, 0.758238, 0.696021, 0.153896],
            [0.815833, 0.224441, 0.223818, 0.536974, 0.592940],
            [0.580086, 0.091487, 0.877461, 0.265600, 0.129515],
            [0.888748, 0.955651, 0.862128, 0.809516, 0.655242],
            [0.550857, 0.086987, 0.408453, 0.372689, 0.259754],
            [0.723420, 0.495876, 0.081046, 0.220183, 0.683259]
        ],
        [
            pd.Timestamp('2018-01-01 00:00:01.897912291'),
            pd.Timestamp('2018-01-01 00:00:02.001118529'),
            pd.Timestamp('2018-01-01 00:00:02.096394939'),
            pd.Timestamp('2018-01-01 00:00:02.197921447'),
            pd.Timestamp('2018-01-01 00:00:02.298663618'),
            pd.Timestamp('2018-01-01 00:00:02.399560700'),
            pd.Timestamp('2018-01-01 00:00:02.502851760'),
            pd.Timestamp('2018-01-01 00:00:02.596996738')
        ]
    )
    assert node.o.meta == expected_meta
    pd.testing.assert_frame_equal(node.o.data, expected_data)


def test_invalid_event():
    # Make sure no epoch is fetched on an invalid trigger
    node.clear()
    node.i.data = data.next()
    time = node.i.data.index[0]
    event = pd.DataFrame([['invalid', 'foobar']], [time], columns=['label', 'data'])
    node.i_events.data = event
    node.update()
    assert node.o.data == None


def test_multiple_events():
    # Make sure the correct onset is caught
    node.clear()
    node.i.data = data.next()
    event = pd.DataFrame(
        [
            ['invalid_1', 'foo'],
            ['test', 'bar'],
            ['invalid_2', 'baz']
        ], [
            node.i.data.index[0],
            node.i.data.index[1],
            node.i.data.index[2]
        ], columns=['label', 'data'])
    node.i_events.data = event
    node.update()
    assert node.o.meta['epoch']['onset'] == pd.Timestamp('2018-01-01 00:00:04.598117111')


def test_multiple_triggers():
    # Mutliple epochs should be returned
    node.clear()
    node.i.data = data.next()
    event = pd.DataFrame(
        [
            ['test', 'foo'],
            ['test', 'bar'],
        ], [
            node.i.data.index[0],
            node.i.data.index[1],
        ], columns=['label', 'data'])
    node.i_events.data = event
    node.update()
    assert node.o_0.meta['epoch']['context'] == 'foo'
    assert node.o_1.meta['epoch']['context'] == 'bar'


def test_unsynced_event():
    # The epoch must be fetched even if the trigger time does not match exactly
    node.clear()
    node.i.data = data.next(20)
    time = pd.Timestamp('2018-01-01 00:00:07.100')
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])
    node.i_events.data = event
    node.update()
    expected_indices = pd.DatetimeIndex([
        pd.Timestamp('2018-01-01T00:00:06.904868869'),
        pd.Timestamp('2018-01-01T00:00:07.002722448'),
        pd.Timestamp('2018-01-01T00:00:07.096987157'),
        pd.Timestamp('2018-01-01T00:00:07.195055221'),
        pd.Timestamp('2018-01-01T00:00:07.303154614'),
        pd.Timestamp('2018-01-01T00:00:07.402068573'),
        pd.Timestamp('2018-01-01T00:00:07.502290072'),
        pd.Timestamp('2018-01-01T00:00:07.602712703'),
        pd.Timestamp('2018-01-01T00:00:07.695740447')
    ])
    pd.testing.assert_index_equal(node.o.data.index, expected_indices)


def test_receives_event_before_data():
    # receives event before data
    # NB: this use case can also happen if before<0
    before = 0
    data.reset()
    node = Epoch(event_trigger='test', before=before, after=.6)
    node.clear()
    node.i.data = data.next(5)
    time = data._data.index[6]  # Sync event right after last sample
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    node.i_events.data = None
    node.i.data = data.next(10)
    node.update()
    expected_data = pd.DataFrame(
        [
            [0.037008, 0.59627, 0.230009, 0.120567, 0.076953],
            [0.696289, 0.339875, 0.724767, 0.065356, 0.31529],
            [0.539491, 0.790723, 0.318753, 0.625891, 0.885978],
            [0.615863, 0.232959, 0.024401, 0.870099, 0.021269],
            [0.874702, 0.528937, 0.939068, 0.798783, 0.997934],
            [0.350712, 0.767188, 0.401931, 0.479876, 0.627505]
        ],
        [
            pd.Timestamp('2018-01-01 00:00:00.595580836'),
            pd.Timestamp('2018-01-01 00:00:00.703661761'),
            pd.Timestamp('2018-01-01 00:00:00.801011149'),
            pd.Timestamp('2018-01-01 00:00:00.902080726'),
            pd.Timestamp('2018-01-01 00:00:00.995205845'),
            pd.Timestamp('2018-01-01 00:00:01.104699099'),
        ]
    )
    expected_meta = {'epoch': {'onset': pd.Timestamp('2018-01-01 00:00:00.595580836'),
                               'context': 'foobar',
                               'before': before,
                               'after': after}}
    assert node.o.meta == expected_meta
    pd.testing.assert_frame_equal(node.o.data, expected_data)


def test_empty_epoch():
    # no data received in the epoch
    before = 1
    after = -1
    data = helpers.DummyData(rate=.1)
    node = Epoch(event_trigger='test', before=before, after=after)
    node.clear()
    node.i.data = data.next(5)
    time = pd.Timestamp("2018-01-01 00:00:10.450714306")  # Sync event to second sample
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    expected_meta = {'epoch': {'onset': pd.Timestamp('2018-01-01 00:00:10.450714306'),
                               'context': 'foobar',
                               'before': before,
                               'after': after}
                     }
    assert node.o.meta == expected_meta
    assert node.o.data.empty


def test_to_xarray():
    """ Test the epoch followed by a conversion to xarray.
    """

    graph = {
        'nodes': [
            {
                'id': 'epoch',
                'module': 'timeflux.nodes.epoch',
                'class': 'Epoch',
                'params': {
                    'before': before,
                    'after': after,
                    'event_trigger': 'test'

                }
            },
            {
                'id': 'to_xarray',
                'module': 'timeflux.nodes.epoch',
                'class': 'ToXArray',
                'params': {
                    'reporting': 'error'
                }
            }
        ],
        'edges': [
            {
                'source': 'epoch:*',
                'target': 'to_xarray'
            }

        ]
    }
    # test with data that has no jitter
    data = helpers.DummyData(jitter=0, rate=rate)
    converted_epoch = Branch(graph)
    converted_epoch.set_port('epoch', port_id='i', data=data.next(20))
    event = pd.DataFrame(
        [
            ['test', 'foo'],
            ['test', 'bar'],
        ], [
            pd.Timestamp('2018-01-01 00:00:00.300000'),
            pd.Timestamp('2018-01-01 00:00:00.400000')
        ], columns=['label', 'data'])
    converted_epoch.set_port('epoch', port_id='i_events', data=event)
    converted_epoch.update()

    expected_meta = {'epochs_context': ['foo', 'bar'], 'rate': 10,
                     'epochs_onset': [pd.Timestamp('2018-01-01 00:00:00.300000'),
                                      pd.Timestamp('2018-01-01 00:00:00.400000')]}
    assert converted_epoch.get_port('to_xarray').meta == expected_meta

    expected_data = xr.DataArray(data=np.array([[[0.658783, 0.692277, 0.849196, 0.249668, 0.489425],
                                                 [0.221209, 0.987668, 0.944059, 0.039427, 0.705575],
                                                 [0.925248, 0.180575, 0.567945, 0.915488, 0.033946],
                                                 [0.69742, 0.297349, 0.924396, 0.971058, 0.944266],
                                                 [0.474214, 0.862043, 0.844549, 0.3191, 0.828915],
                                                 [0.037008, 0.59627, 0.230009, 0.120567, 0.076953],
                                                 [0.696289, 0.339875, 0.724767, 0.065356, 0.31529],
                                                 [0.539491, 0.790723, 0.318753, 0.625891, 0.885978],
                                                 [0.615863, 0.232959, 0.024401, 0.870099, 0.021269]],
                                                [[0.221209, 0.987668, 0.944059, 0.039427, 0.705575],
                                                 [0.925248, 0.180575, 0.567945, 0.915488, 0.033946],
                                                 [0.69742, 0.297349, 0.924396, 0.971058, 0.944266],
                                                 [0.474214, 0.862043, 0.844549, 0.3191, 0.828915],
                                                 [0.037008, 0.59627, 0.230009, 0.120567, 0.076953],
                                                 [0.696289, 0.339875, 0.724767, 0.065356, 0.31529],
                                                 [0.539491, 0.790723, 0.318753, 0.625891, 0.885978],
                                                 [0.615863, 0.232959, 0.024401, 0.870099, 0.021269],
                                                 [0.874702, 0.528937, 0.939068, 0.798783, 0.997934]]]),
                                 dims=('epoch', 'time', 'space'),
                                 coords=([0, 1],
                                         pd.TimedeltaIndex(np.arange(-before, after + 1 / rate, 1 / rate), unit='s'),
                                         [0, 1, 2, 3, 4]))
    xr.testing.assert_equal(converted_epoch.get_port('to_xarray').data, expected_data)

    # now test with jittered data, so that the some epoch have invalid number of samples
    data = helpers.DummyData(jitter=.2, rate=rate)
    converted_epoch = Branch(graph)
    converted_epoch.set_port('epoch', port_id='i', data=data.next(20))
    event = pd.DataFrame(
        [
            ['test', 'foo'],
            ['test', 'bar'],
        ], [
            pd.Timestamp('2018-01-01 00:00:00.300000'),
            pd.Timestamp('2018-01-01 00:00:00.400000')
        ], columns=['label', 'data'])
    converted_epoch.set_port('epoch', port_id='i_events', data=event)

    with pytest.raises(WorkerInterrupt):
        converted_epoch.update()
