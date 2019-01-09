"""Tests for query nodes"""

import pytest
import pandas as pd
import numpy as np
from timeflux.core.registry import Registry
import helpers


Registry.cycle_start = 0
Registry.rate = 1

from timeflux.nodes.query import *

fs=10
data = helpers.DummyData( rate=fs, jitter=.05, num_cols=6)
all_data = data._data
chunk_size = 3



def test_locquery():
    data.reset()
    data._data.columns = ['A', 'B', 'C', 'D', 'E', 'F']
    node = LocQuery(key=['A', 'E'], axis = 1)
    node.i.data = data.next(chunk_size)
    node.update()
    pd.testing.assert_frame_equal(node.o.data, all_data.loc[:, ["A", "E"]][:chunk_size] )

    # test query with wrong key: "R" not in the input columns
    with pytest.raises(ValueError):
        node = LocQuery(key=['R'], axis=1)
        node.i.data = data.next()
        node.update()


def test_xsquery():
    data.reset()
    data._data.columns = pd.MultiIndex.from_product([['A', 'B'], [1,2,3]], names=["first", "second"])
    node = XsQuery(key=(1, 'B'), axis = 1, level= ["second", "first"], drop_level=False)
    node.i.data = data.next(chunk_size)
    node.update()
    pd.testing.assert_frame_equal(node.o.data, all_data.loc[:, [("B", 1)]][:chunk_size])

    # test query with wrong key: "R" not in the input columns
    with pytest.raises(ValueError):
        node = LocQuery(key=['R'], axis=1)
        node.i.data = data.next()
        node.update()

def test_selectrange():
    data.reset()
    node = SelectRange(ranges={"second": [1,1.5]}, axis= 1, inclusive=True)
    data._data.columns = pd.MultiIndex.from_product([['A', 'B'], [1.3,1.6, 1.9]], names=["first", "second"])
    node.i.data = data.next(chunk_size)
    node.update()
    pd.testing.assert_frame_equal(node.o.data, all_data.xs((1.3), level= "second", axis=1, drop_level=False)[:chunk_size])

a =1