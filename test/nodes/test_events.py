"""Tests for events.py"""

import pytest
import logging
import pandas as pd
from timeflux.nodes.events import EventToSignal
from timeflux.helpers.port import make_event

def test_empty():
    node = EventToSignal(label="foobar")
    node.i.data = make_event("marker", 42, serialize=False)
    node.update()
    assert node.o.data == None

def test_scalar():
    node = EventToSignal()
    node.i.data = make_event("marker", 42, serialize=False)
    node.update()
    assert node.o.data["marker"].iloc[0] == 42

def test_dict():
    node = EventToSignal(keys=("meaning", "life"))
    node.i.data = make_event("marker", {"meaning": {"life": 42}}, serialize=False)
    node.update()
    assert node.o.data["marker"].iloc[0] == 42

def test_json():
    node = EventToSignal(keys=("meaning", "life"))
    node.i.data = make_event("marker", {"meaning": {"life": 42}}, serialize=True)
    node.update()
    assert node.o.data["marker"].iloc[0] == 42

def test_invalid_keys():
    node = EventToSignal(keys=("meaning", "life"))
    node.i.data = make_event("marker", {"meaning_of_life":42}, serialize=False)
    node.update()
    assert node.o.data["marker"].iloc[0] == None

def test_invalid_json():
    node = EventToSignal(keys=("meaning", "life"))
    node.i.data = make_event("marker", '{"meaning_of_life":42', serialize=False)
    node.update()
    assert node.o.data["marker"].iloc[0] == None

def test_multiple():
    node = EventToSignal()
    index = [pd.Timestamp('2023-01-01 00:00:00.000'), pd.Timestamp('2023-01-01 00:00:00.100')]
    label = ["marker", "marker"]
    data = [1, 2]
    node.i.data = pd.DataFrame({"label": label, "data": data}, index=index)
    node.update()
    assert len(node.o.data) == 2
    assert node.o.data["marker"].iloc[0] == 1
    assert node.o.data["marker"].iloc[1] == 2
