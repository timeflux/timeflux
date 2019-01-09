"""Tests for Expression node"""

import pytest
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
from timeflux.core.registry import Registry
from datetime import datetime

Registry.cycle_start = 0
Registry.rate = 1

from timeflux.core.io import Port
from timeflux.nodes.expression import Expression



def test_expression_on_ports_case1():
    ## test with a expression to eval on the input ports (eval_on = "ports")
    node = Expression(expr="i_1 + i_2", eval_on="ports", kwargs={})
    node.ports = {}
    node.ports["i_1"] = Port()
    node.ports["i_2"] = Port()
    node.ports["i_1"].data = pd.DataFrame(data=[[5, 8], [9, 5], [10, 4], [5, 5]],
                               index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in  np.arange(4)])
    node.ports["i_2"].data = pd.DataFrame(data=[[1, 3], [1, 5], [10, 1], [2, 1]],
                               index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in  np.arange(4)])

    node.update()
    expected = pd.DataFrame(data=[[5+1, 8+3], [9+1, 5+5], [10+10, 4+1], [5+2, 5+1]],
                               index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in  np.arange(4)])
    assert_frame_equal(node.o.data, expected)


def test_expression_on_ports_case2():
    ## test with a expression to eval on the input ports (eval_on = "ports")
    node = Expression(expr="i_1 > 6", eval_on="ports", kwargs={})
    node.ports = {}
    node.ports["i_1"] = Port()
    node.ports["i_1"].data = pd.DataFrame(data=[[5, 8], [9, 5]],
                               index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in
                                      np.arange(2)],
                               columns=["col0", "col1"])
    node.update()
    expected = pd.DataFrame(data=[[5>6, 8>6], [9>6, 5>6 ]],
                            index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in
                                   np.arange(2)],
                            columns=["col0", "col1"])

    assert_frame_equal(node.o.data, expected)

def test_expression_on_columns_case1():
    ## test with a expression to eval on the input ports (eval_on = "columns")
    node = Expression(expr="col2 = col0 + col1", eval_on="columns", kwargs={})
    node.i.data = pd.DataFrame(data=[[5, 8], [9, 5]],
                               index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in  np.arange(2)],
                               columns=["col0", "col1"])
    node.update()
    expected = pd.DataFrame(data=[[5, 8, 5+8], [9, 5, 9+5]],
                               index=[np.datetime64(datetime.utcfromtimestamp(timestamp), 'us') for timestamp in  np.arange(2)],
                               columns=["col0", "col1", "col2"])

    assert_frame_equal(node.o.data, expected)