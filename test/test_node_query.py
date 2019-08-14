"""Tests for query nodes"""

import pandas as pd
import pytest
from timeflux.core.exceptions import WorkerInterrupt
from timeflux.helpers.testing import DummyData
from timeflux.nodes.query import LocQuery, SelectRange, XsQuery

fs = 10
data = DummyData(rate=fs, jitter=.05, num_cols=6)
all_data = data._data


def test_locquery():
    data.reset()
    data._data.columns = ['A', 'B', 'C', 'D', 'E', 'F']
    node = LocQuery(key=('A', 'E'), axis=1)
    node.i.data = data.next(3)
    node.update()
    expected_data = pd.DataFrame(
        [
            [0.185133, 0.806561],
            [0.692277, 0.221209],
            [0.944059, 0.180575]
        ],
        [
            pd.Timestamp('2017-12-31 23:59:59.998745401'),
            pd.Timestamp('2018-01-01 00:00:00.104507143'),
            pd.Timestamp('2018-01-01 00:00:00.202319939'),

        ],
        ['A', 'E']
    )
    pd.testing.assert_frame_equal(node.o.data, expected_data)

    # test query with wrong key: "R" not in the input columns
    with pytest.raises(WorkerInterrupt):
        node = LocQuery(key=['R'], axis=1)
        node.i.data = data.next()
        node.update()


def test_xsquery():
    data.reset()
    data._data.columns = pd.MultiIndex.from_product([['A', 'B'], [1, 2, 3]], names=['first', 'second'])
    node = XsQuery(key=(1, 'B'), axis=1, level=['second', 'first'], drop_level=False)
    node.i.data = data.next(3)
    node.update()
    expected_data = pd.DataFrame(
        [[0.732225],
         [0.489425],
         [0.925248]],
        [
            pd.Timestamp('2017-12-31 23:59:59.998745401'),
            pd.Timestamp('2018-01-01 00:00:00.104507143'),
            pd.Timestamp('2018-01-01 00:00:00.202319939'),

        ],
        pd.MultiIndex.from_product([['B'], [1]], names=['first', 'second'])
    )
    pd.testing.assert_frame_equal(node.o.data, expected_data)


def test_selectrange():
    data.reset()
    node = SelectRange(ranges={'second': [1, 1.5]}, axis=1, inclusive=True)
    data._data.columns = pd.MultiIndex.from_product([['A', 'B'], [1.3, 1.6, 1.9]],
                                                    names=['first', 'second'])
    node.i.data = data.next(3)
    node.update()
    expected_data = pd.DataFrame(
        [[0.185133, 0.732225],
         [0.692277, 0.489425],
         [0.944059, 0.925248]],
        [
            pd.Timestamp('2017-12-31 23:59:59.998745401'),
            pd.Timestamp('2018-01-01 00:00:00.104507143'),
            pd.Timestamp('2018-01-01 00:00:00.202319939'),

        ],
        pd.MultiIndex.from_product([['A', 'B'], [1.3]], names=['first', 'second'])
    )

    pd.testing.assert_frame_equal(node.o.data, expected_data)
