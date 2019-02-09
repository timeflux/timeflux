"""Tests for query nodes"""

import pytest
import pandas as pd
import numpy as np
from timeflux.core.registry import Registry
import helpers
import xarray as xr

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


def test_averagebands():
    # here, we create an xarray with thre axis: (time, freq, space)
    # freq is (freq) [1 3 5 7 9 11 13 15 17 19 21 23 25 27 29 31 33 35 37 39]
    # time is (time) datetime64[ns] 2018-01-01 ... 2018-01-01T00:00:00.400000
    # space is (space) <U6 'simple' 'double'
    # we fill the array with X(freq=f, space='simple')=f and X(freq=f, space='double')=2*f
    # hence, byaveraging in band 'alpha:=[8,12], we expect a dataframe with columns ['simple' 'double']
    # and with respective values, the average between 9 and 11 (ie. 10) and the average between 9*2 and 10*2 (ie. 20)
    Nt = 5
    Nf = 20
    node = AverageBands(columns_dimension="space", band_dimension="freq",
                        bands=[{"name": "alpha", "range": [8, 12]}, {"name": "beta", "range": [12, 30]}])
    freq = np.arange(1, Nf * 2, 2)
    space = ["simple", "double"]
    time = pd.date_range(
        start='2018-01-01',
        periods=Nt,
        freq=pd.DateOffset(seconds=.1))
    data = np.stack([np.repeat(np.array(freq).reshape(-1, 1), Nt, axis=1).T,
                     np.repeat(np.array(freq * 2).reshape(-1, 1), Nt, axis=1).T], 2)
    node.i.data = xr.DataArray(data,
                               coords=[time, freq, space],
                               dims=['time', 'freq', 'space'])
    node.update()

    expected_freq = np.array([0., 4.16666667, 8.33333333, 12.5, 16.66666667,
                              20.83333333, 25., 29.16666667, 33.33333333, 37.5,
                              41.66666667, 45.83333333, 50.])
    expected_data_alpha = np.array(
        [[10., 20.],
         [10., 20.],
         [10., 20.],
         [10., 20.],
         [10., 20.]])
    expected_data_beta = np.array([[21., 42.],
                                   [21., 42.],
                                   [21., 42.],
                                   [21., 42.],
                                   [21., 42.]])

    alpha_expected = pd.DataFrame(data=expected_data_alpha, columns=["simple", "double"], index=time)
    beta_expected = pd.DataFrame(data=expected_data_beta, columns=["simple", "double"], index=time)

    pd.testing.assert_frame_equal(alpha_expected, node.o_alpha.data)
    pd.testing.assert_frame_equal(beta_expected, node.o_beta.data)


