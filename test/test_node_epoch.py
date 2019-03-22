"""Tests for epoch.py"""

import pandas as pd
import helpers
from timeflux.nodes.epoch import Epoch

data = helpers.DummyData()
node = Epoch(event_trigger='test', before=.2, after=.6)


def test_no_epoch():
    # Send enough data for an epoch, but no event
    node.i.data = data.next(20)
    node.update()
    assert node.o.data == None

def test_send_event_short_data():
    # Send an event trigger, but not enough data to capture the epoch in one round
    node.clear()
    node.i.data = data.next(5)
    time = node.i.data.index[1] # Sync event to second sample
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data']) # Generate a trigger event
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
            'context': 'foobar'
        }
    }
    expected_data = pd.DataFrame(
        [
           [0.176528, 0.220486, 0.186438, 0.779584, 0.350125],
           [0.057843, 0.969103, 0.883786, 0.927752, 0.994908],
           [0.173895, 0.396242, 0.758238, 0.696021, 0.153896],
           [0.815833, 0.224441, 0.223818, 0.536974, 0.59294 ],
           [0.580086, 0.091487, 0.877461, 0.2656  , 0.129515],
           [0.888748, 0.955651, 0.862128, 0.809516, 0.655242],
           [0.057843, 0.969103, 0.883786, 0.927752, 0.994908],
           [0.173895, 0.396242, 0.758238, 0.696021, 0.153896],
           [0.815833, 0.224441, 0.223818, 0.536974, 0.59294 ],
           [0.580086, 0.091487, 0.877461, 0.2656  , 0.129515],
           [0.888748, 0.955651, 0.862128, 0.809516, 0.655242],
           [0.550857, 0.086987, 0.408453, 0.372689, 0.259754],
           [0.72342 , 0.495876, 0.081046, 0.220183, 0.683259]
        ],
        [
            pd.Timestamp('2018-01-01 00:00:01.897912291'),
            pd.Timestamp('2018-01-01 00:00:02.001118529'),
            pd.Timestamp('2018-01-01 00:00:02.096394939'),
            pd.Timestamp('2018-01-01 00:00:02.197921447'),
            pd.Timestamp('2018-01-01 00:00:02.298663618'),
            pd.Timestamp('2018-01-01 00:00:02.399560700'),
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
    node = Epoch(event_trigger='test', before=.1, after=.2)
    data.reset()

    node.clear()
    node.i.data = data.next(20)
    time = pd.Timestamp('2018-01-01 00:00:00.55')
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])
    node.i_events.data = event
    node.update()

    expected_indices = pd.DatetimeIndex([
        pd.Timestamp('2018-01-01 00:00:00.496559945'),
        pd.Timestamp('2018-01-01 00:00:00.595580836'),
        pd.Timestamp('2018-01-01 00:00:00.703661761'),
        pd.Timestamp('2018-01-01 00:00:00.496559945'),
        pd.Timestamp('2018-01-01 00:00:00.595580836'),
        pd.Timestamp('2018-01-01 00:00:00.703661761'),
    ])
    pd.testing.assert_index_equal(node.o.data.index, expected_indices)


def test_receives_event_before_data():
    # receives event before data
    # NB: this use case can also happen if before<0
    data.reset()
    node = Epoch(event_trigger='test', before=0, after=.6)
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
    expected_meta = {'epoch': {'onset': pd.Timestamp('2018-01-01 00:00:00.595580836'), 'context': 'foobar'}}
    assert node.o.meta == expected_meta
    pd.testing.assert_frame_equal(node.o.data, expected_data)

def test_empty_epoch():
    # no data received in the epoch
    data = helpers.DummyData(rate=.1)
    node = Epoch(event_trigger='test', before=-1, after=1)
    node.clear()
    node.i.data = data.next(5)
    time = pd.Timestamp("2018-01-01 00:00:10.450714306")  # Sync event to second sample
    event = pd.DataFrame([['test', 'foobar']], [time], columns=['label', 'data'])  # Generate a trigger event
    node.i_events.data = event
    node.update()
    expected_meta = {'epoch': {'onset': pd.Timestamp('2018-01-01 00:00:10.450714306'), 'context': 'foobar'}}
    assert node.o.meta == expected_meta
    assert node.o.data.empty