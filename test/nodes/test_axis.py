"""Tests for axis.py"""
import pandas as pd

from timeflux.helpers.testing import DummyData
from timeflux.nodes.axis import AddSuffix, Rename, RenameColumns

pandas_data = DummyData(cols=['0', '1', '2'])


def test_add_suffix():
    node = AddSuffix(suffix='_foo')
    pandas_data.reset()
    node.i.data = pandas_data.next(20)
    node.update()

    pd.testing.assert_index_equal(node.o.data.columns, pd.Index(['0_foo', '1_foo', '2_foo']))


def test_rename():
    node = Rename(mapper={'0': 'new_0'}, axis=1)
    pandas_data.reset()
    node.i.data = pandas_data.next(20)
    node.update()
    pd.testing.assert_index_equal(node.o.data.columns, pd.Index(['new_0', '1', '2']))


def test_rename_columns():
    # wrong length of parameter `list`
    node = RenameColumns(names=['foo'])
    pandas_data.reset()
    node.i.data = pandas_data.next(20)
    node.update()
    assert node.o.data is None
    # correct length of parameter `list`
    node = RenameColumns(names=['A', 'B', 'C'])
    node.i.data = pandas_data.next(20)
    node.update()
    pd.testing.assert_index_equal(node.o.data.columns, pd.Index(['A', 'B', 'C']))
