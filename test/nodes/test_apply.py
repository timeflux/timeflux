"""Tests for Apply node """

from datetime import datetime

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from timeflux.nodes.apply import ApplyMethod

test_data = pd.DataFrame(data=[[5, 8], [9, 5], [10, 4], [5, 5]],
                         index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us')
                                for timestamp in np.arange(4)])


def test_apply_universal():
    """test with a "universal" numpy function"""
    node = ApplyMethod(method='numpy.square', apply_mode='universal')

    node.i.data = test_data
    node.update()
    expected = pd.DataFrame(data=[[5 ** 2, 8 ** 2], [9 ** 2, 5 ** 2], [10 ** 2, 4 ** 2], [5 ** 2, 5 ** 2]],
                            index=test_data.index)
    assert_frame_equal(node.o.data, expected)


def test_apply_reduce():
    """test with a "reduce" numpy function on on horizontal axis"""
    node = ApplyMethod(method='numpy.sum', apply_mode="reduce", closed='right', axis=0)

    node.i.data = test_data
    node.update()
    expected = pd.DataFrame(data=np.array([[5 + 9 + 10 + 5], [8 + 5 + 4 + 5]]).reshape(1, -1),
                            index=[test_data.index[3]])

    assert_frame_equal(node.o.data, expected)

    with pytest.raises(TypeError):
        # introduce an unexpected additional arguments a in kwargs
        node = ApplyMethod(method='numpy.sum', apply_mode='reduce', closed='right',
                           axis=1, a=1)
        node.i.data = test_data
        node.update()
    with pytest.raises(ValueError):
        # miss-spell the method
        node = ApplyMethod(method='numpy.summ', apply_mode='reduce', closed='right', axis=1)
        node.i.data = test_data
        node.update()
