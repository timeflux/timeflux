"""Tests for Apply node """

import pytest
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
from timeflux.core.registry import Registry
from datetime import datetime

Registry.cycle_start = 0
Registry.rate = 1

from timeflux.nodes.apply import ApplyMethod

## test with a "universal" numpy function
node = ApplyMethod(module_name="numpy", method_name="square", method_type="universal" )

node.i.data = pd.DataFrame(data=[[5, 8], [9, 5], [10, 4], [5, 5]],
                           index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in  np.arange(4)])
node.update()
expected = pd.DataFrame(data=[[5**2, 8**2], [9**2, 5**2], [10**2, 4**2], [5**2, 5**2]],
                           index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in np.arange(4)])
assert_frame_equal(node.o.data, expected)


## test with a "reduce" numpy function on on vertical axis
node = ApplyMethod(module_name="numpy", method_name="sum", method_type="reduce", closed="right", axis=0)

node.i.data = pd.DataFrame(data=[[5, 8], [9, 5], [10, 4], [5, 5]],
                           index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in np.arange(4)])
node.update()
expected = pd.DataFrame(data=np.array([[5+9+10+5], [8+5+4+5]]).reshape(1,-1), index =[np.datetime64(datetime.utcfromtimestamp(3), 'us')] )

assert_frame_equal(node.o.data, expected)


def test_apply_universal():
    ## test with a "universal" numpy function
    node = ApplyMethod(module_name="numpy", method_name="square", method_type="universal" )

    node.i.data = pd.DataFrame(data=[[5, 8], [9, 5], [10, 4], [5, 5]],
                               index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in  np.arange(4)])
    node.update()
    expected = pd.DataFrame(data=[[5**2, 8**2], [9**2, 5**2], [10**2, 4**2], [5**2, 5**2]],
                               index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in np.arange(4)])
    assert_frame_equal(node.o.data, expected)


    ## test with a "reduce" numpy function on on vertical axis
    node = ApplyMethod(module_name="numpy", method_name="sum", method_type="reduce", closed="right", axis=0)

    node.i.data = pd.DataFrame(data=[[5, 8], [9, 5], [10, 4], [5, 5]],
                               index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in np.arange(4)])
    node.update()
    expected = pd.DataFrame(data=np.array([[5+9+10+5], [8+5+4+5]]).reshape(1,-1), index =[np.datetime64(datetime.utcfromtimestamp(3), 'us')] )

    assert_frame_equal(node.o.data, expected)


def test_apply_reduce():

    ## test with a "reduce" numpy function on on horizontal axis
    node = ApplyMethod(module_name="numpy", method_name="sum", method_type="reduce", closed="right", axis=1)

    node.i.data = pd.DataFrame(data=[[5, 8], [9, 5], [10, 4], [5, 5]],
                               index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in np.arange(4)])
    node.update()
    expected = pd.DataFrame(data=np.array([[5+8], [9+5], [10+4], [5+5]]), index =[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in np.arange(4)] )
    assert_frame_equal(node.o.data, expected)

    with pytest.raises(TypeError):
        node = ApplyMethod(module_name="numpy", method_name="sum", method_type="reduce", closed="right", axis=1, kwds={"a":1})
        node.i.data = pd.DataFrame(data=[[5, 8], [9, 5], [10, 4], [5, 5]],
                                   index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in
                                          np.arange(4)])
        node.update()
    with pytest.raises((ValueError,TypeError  )):
            node = ApplyMethod(module_name="numpy", method_name="summ", method_type="reduce", closed="right", axis=1)
            node.i.data = pd.DataFrame(data=[[5, 8], [9, 5], [10, 4], [5, 5]],
                                       index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in  np.arange(4)])
            node.update()

