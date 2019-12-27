import pytest
import pandas as pd
import numpy as np
from timeflux.core.io import Port
from timeflux.helpers.port import match_events, get_meta

def test_match_events():
    port = Port()
    events = [['foo', ''], ['bar', ''], ['baz', ''], ['foo', '']]
    times = pd.date_range(start='2018-01-01', periods=4, freq='1s')
    port.set(events, times, ['label', 'data'])
    indices = match_events(port, 'foo').index.values.tolist()
    expected = [1514764800000000000, 1514764803000000000]
    assert indices == expected

def test_match_empty():
    port = Port()
    events = [['foo', ''], ['bar', '']]
    times = pd.date_range(start='2018-01-01', periods=2, freq='1s')
    port.set(events, times, ['label', 'data'])
    assert match_events(port, 'baz') == None

def test_get_meta():
    port = Port()
    port.meta = {'foo': {'bar': {'baz': 42}}}
    assert get_meta(port, ('foo', 'bar', 'baz')) == 42

def test_get_meta_not_found():
    port = Port()
    assert get_meta(port, ('foo')) == None
    assert get_meta(port, ('foo'), False) == False