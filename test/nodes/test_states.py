"""Tests for states.py"""
import numpy as np
import pandas as pd
from timeflux.helpers.testing import DummyData
from timeflux.nodes.states import States

pandas_data = DummyData()

# def test_states_no_events():
#     pandas_data.reset()
#     node = States(initial='foo', states=['foo', 'bar'], event_label='label')
#     # Send data but no event
#     node.i.data = pandas_data.next(20)
#     node.i.data['label'] = np.NaN
#     node.update()
#     expected_states = pd.DataFrame({'state': 'foo'}, index=node.i.data.index)
#     pd.testing.assert_frame_equal(node.o.data, expected_states)


def test_states_no_relevant_events():
    pandas_data.reset()
    node = States(initial='foo', states=['foo', 'bar', 'baz'], transitions={'baz': {'from': 'bar', 'to':'baz'}}, event_label='label')
    node.i.data = pandas_data.next(20)
    # The 'foo' event is irrelevant because we are already in state foo
    node.i.data.loc[node.i.data.index[3], 'label'] = 'foo'
    # The 'baz' event is irrelevant because the 'baz' transition only goes from the state 'bar'
    node.i.data.loc[node.i.data.index[10], 'label'] = 'baz'
    node.update()
    expected_states = pd.DataFrame({'state': 'foo'}, index=node.i.data.index)
    pd.testing.assert_frame_equal(node.o.data, expected_states)

def test_states_with_relevant_events():
    pandas_data.reset()
    node = States(initial='foo', states=['foo', 'bar', 'baz'], transitions={'bump': {'from': 'bar', 'to':'baz'}}, event_label='label')
    node.i.data = pandas_data.next(20)
    expected_states = pd.DataFrame({'state': 'foo'}, index=node.i.data.index)
    expected_transitions = pd.DataFrame(columns=['transition'])

    node.i.data.loc[node.i.data.index[3], 'label'] = 'bar'
    node.i.data.loc[node.i.data.index[4], 'label'] = 'bar'
    node.i.data.loc[node.i.data.index[10], 'label'] = 'bump'
    expected_states.iloc[3:] = 'bar'
    expected_states.iloc[10:] = 'baz' # state name, not transition name 'bump'
    expected_transitions.loc[node.i.data.index[3], 'transition'] = 'bar'
    # bar is NOT a valid transition from bar to bar because reflexive_transitions is False by default.
    expected_transitions.loc[node.i.data.index[10], 'transition'] = 'bump' # transition name, not state name 'baz'


    node.update()
    pd.testing.assert_frame_equal(node.o.data, expected_states)
    pd.testing.assert_frame_equal(node.o_transitions.data, expected_transitions)

    node.i.data = pandas_data.next(20)
    expected_states = pd.DataFrame({'state': 'baz'}, index=node.i.data.index)
    expected_transitions = pd.DataFrame(columns=['transition'])

    node.i.data.loc[node.i.data.index[4], 'label'] = 'bar'
    node.i.data.loc[node.i.data.index[6], 'label'] = 'foo'
    expected_states.iloc[4:] = 'bar'
    expected_states.iloc[6:] = 'foo'
    expected_transitions.loc[node.i.data.index[4], 'transition'] = 'bar'
    expected_transitions.loc[node.i.data.index[6], 'transition'] = 'foo'

    node.update()
    pd.testing.assert_frame_equal(node.o.data, expected_states)
    pd.testing.assert_frame_equal(node.o_transitions.data, expected_transitions)

def test_states_with_reflexive_transitions():
    pandas_data.reset()
    node = States(initial='foo', states=['foo', 'bar', 'baz'], event_label='label', reflexive_transitions=True)
    node.i.data = pandas_data.next(20)
    expected_states = pd.DataFrame({'state': 'foo'}, index=node.i.data.index)
    expected_transitions = pd.DataFrame(columns=['transition'])

    node.i.data.loc[node.i.data.index[3], 'label'] = 'bar'
    node.i.data.loc[node.i.data.index[4], 'label'] = 'bar'
    node.i.data.loc[node.i.data.index[10], 'label'] = 'baz'
    expected_states.iloc[3:] = 'bar'
    expected_states.iloc[10:] = 'baz' # state name, not transition name 'bump'
    expected_transitions.loc[node.i.data.index[3], 'transition'] = 'bar'
    expected_transitions.loc[node.i.data.index[4], 'transition'] = 'bar' # bar is a valid transition from bar to bar
    expected_transitions.loc[node.i.data.index[10], 'transition'] = 'baz'

    node.update()
    pd.testing.assert_frame_equal(node.o.data, expected_states)
    pd.testing.assert_frame_equal(node.o_transitions.data, expected_transitions)

def test_states_with_differently_indexed_events():
    pandas_data.reset()
    node = States(initial='foo', states=['foo', 'bar', 'baz'], event_label='label')
    node.i.data = pandas_data.next(20)
    # The events data are sampled at half the rate of the main input data, and 25ms later.
    events_index = node.i.data.resample('200l', loffset=pd.Timedelta(225, 'ms')).sum().index
    node.i_events.data = pd.DataFrame(columns=['label'], index=events_index)
    expected_states = pd.DataFrame({'state': 'foo'}, index=node.i.data.index)

    node.i_events.data.loc[node.i_events.data.index[3], 'label'] = 'bar'
    expected_states.iloc[7:] = 'bar'
    node.i_events.data.loc[node.i_events.data.index[6], 'label'] = 'baz'
    expected_states.iloc[13:] = 'baz'
    node.i_events.data.loc[node.i_events.data.index[10], 'label'] = 'foo'# This one falls beyond the last index of node.i.data

    node.update()
    pd.testing.assert_frame_equal(node.o.data, expected_states)

    # The last event is out of scope so is carried over and automatically applied on the next iteration
    node.i.data = pandas_data.next(20)
    # The carried over event is after the first new row of i.
    expected_states = pd.DataFrame({'state': 'baz'}, index=node.i.data.index)
    expected_states.iloc[1:] = 'foo'
    node.update()
    pd.testing.assert_frame_equal(node.o.data, expected_states)


def test_states_without_default_index():
    pandas_data.reset()
    node = States(initial='foo', states=['foo', 'bar', 'baz'], event_label='label')
    data = pandas_data.next(2)
    expected_states = pd.DataFrame(columns=['state'], index=data.index[0:1])

    data.loc[data.index[0], 'label'] = 'bar'
    data.loc[data.index[1], 'label'] = 'baz'
    expected_states.loc[data.index[0], 'state'] = 'bar'
    expected_states.loc[data.index[1], 'state'] = 'baz'
    node.i_events.data = data
    node.update()
    pd.testing.assert_frame_equal(node.o.data, expected_states)

    data = pandas_data.next(2)
    expected_states = pd.DataFrame(columns=['state'], index=data.index[0:1])

    data.loc[data.index[0], 'label'] = 'foo'
    data.loc[data.index[1], 'label'] = 'baz'
    expected_states.loc[data.index[0], 'state'] = 'foo'
    expected_states.loc[data.index[1], 'state'] = 'baz'
    node.i_events.data = data
    node.update()
    pd.testing.assert_frame_equal(node.o.data, expected_states)

def test_states_with_min_seconds():
    pandas_data.reset()
    node = States(initial='foo', states=['foo', 'bar', 'baz'], properties={'bar': {'min_seconds': 0.3}}, event_label='label')
    node.i.data = pandas_data.next(20)
    expected_states = pd.DataFrame({'state': 'foo'}, index=node.i.data.index)

    node.i.data.loc[node.i.data.index[3], 'label'] = 'bar'
    node.i.data.loc[node.i.data.index[5], 'label'] = 'baz'
    node.i.data.loc[node.i.data.index[10], 'label'] = 'baz'
    node.i.data.loc[node.i.data.index[19], 'label'] = 'bar'
    expected_states.iloc[3:] = 'bar'
    # The first baz event at 5 should be blocked by min_seconds on bar
    expected_states.iloc[10:] = 'baz'
    expected_states.iloc[19:] = 'bar'

    node.update()
    pd.testing.assert_frame_equal(node.o.data, expected_states)
    node.i.data = pandas_data.next(20)
    expected_states = pd.DataFrame({'state': 'bar'}, index=node.i.data.index)

    node.i.data.loc[node.i.data.index[1], 'label'] = 'foo'
    node.i.data.loc[node.i.data.index[6], 'label'] = 'baz'
    # The foo event at 1 should be blocked by min_seconds carried over form previous iteration
    expected_states.iloc[6:] = 'baz'

    node.update()
    pd.testing.assert_frame_equal(node.o.data, expected_states)

def test_states_with_auto_transitions():
    pandas_data.reset()
    node = States(initial='foo', states=['foo', 'bar', 'baz'], properties={'bar': {'auto_transition': 'foo', 'auto_transition_seconds': 0.3}}, event_label='label')
    node.i.data = pandas_data.next(20)
    node.i.data.loc[:,'label'] = np.NaN
    expected_states = pd.DataFrame({'state': 'foo'}, index=node.i.data.index)

    node.i.data.loc[node.i.data.index[3], 'label'] = 'bar'
    expected_states.iloc[3:] = 'bar'
    # Auto transition after 0.3 seconds
    expected_states.iloc[7:] = 'foo'
    node.i.data.loc[node.i.data.index[19], 'label'] = 'bar'
    expected_states.iloc[19:] = 'bar'
    # No time for auto-transition on this iteration

    node.update()
    pd.testing.assert_frame_equal(node.o.data, expected_states)
    node.i.data = pandas_data.next(20)
    node.i.data.loc[:,'label'] = np.NaN
    expected_states = pd.DataFrame({'state': 'bar'}, index=node.i.data.index)

    # The carried over auto-transition from bar to foo
    expected_states.iloc[2:] = 'foo'

    node.update()
    pd.testing.assert_frame_equal(node.o.data, expected_states)
