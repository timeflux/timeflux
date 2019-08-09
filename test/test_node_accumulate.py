"""Tests for accumulate.py"""
import pandas as pd
import xarray as xr
from timeflux.helpers.testing import DummyData, DummyXArray
from timeflux.nodes.accumulate import AppendDataFrame, AppendDataArray

xarray_data = DummyXArray()
pandas_data = DummyData()


def test_append_dataframe():
    """"Test node AppendDataFrame"""

    node = AppendDataFrame()
    pandas_data.reset()
    node.clear()
    # gate is not closed, data should be accumulated but not released
    # first chunk
    node.i.data = pandas_data.next(5)
    node.update()
    # assert no output
    assert node.o.data == None
    # assert the data has been buffered
    pd.testing.assert_frame_equal(pandas_data._data.iloc[:5, :], node._data)
    # second chunk
    node.clear()
    node.i.data = pandas_data.next(10)
    node.update()
    # assert no output
    assert node.o.data == None
    # assert the buffer is the concatenation of the 2 accumulated chunks
    pd.testing.assert_frame_equal(pandas_data._data.iloc[:15, :], node._data)
    # now a meta is received, assessing that the gate has just closed
    node.i.data = pandas_data.next(5)
    node.i.meta = {'gate_status': 'closed'}
    node.update()
    # assert output data is the concatenation of the 3 chunks
    pd.testing.assert_frame_equal(pandas_data._data.iloc[:20, :], node.o.data)


def test_append_dataarray():
    """"Test node AppendDataArray"""

    node = AppendDataArray(dim='time')
    xarray_data.reset()
    node.clear()
    # gate is not closed, data should be accumulated but not released
    # first chunk
    node.i.data = xarray_data.next(5)
    node.update()
    # assert no output
    assert node.o.data == None
    # assert the data has been buffered
    xr.testing.assert_equal(xarray_data._data.isel({'time': slice(0, 5)}), node._data_list[0])
    # second chunk
    node.clear()
    node.i.data = xarray_data.next(10)
    node.update()
    # assert no output
    assert node.o.data == None
    # assert the buffer is the concatenation of the 2 accumulated chunks
    xr.testing.assert_equal(xarray_data._data.isel({'time': slice(5, 15)}), node._data_list[1])

    # now a meta is received, assessing that the gate has just closed
    node.i.data = xarray_data.next(5)
    node.i.meta = {'gate_status': 'closed'}
    node.update()
    # assert output data is the concatenation of the 3 chunks
    xr.testing.assert_equal(xarray_data._data.isel({'time': slice(0, 20)}), node.o.data)
