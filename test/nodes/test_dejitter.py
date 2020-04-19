import numpy as np
import pandas as pd
import pytest
import logging
from timeflux.core.exceptions import WorkerInterrupt
from timeflux.helpers.testing import DummyData, Looper
from timeflux.nodes.dejitter import Snap, Interpolate

rate = 10

dummy_data_with_jitter = DummyData(rate=rate, jitter=.05)

num_cols = 5
dummy_data_no_jitter = DummyData(rate=rate, jitter=0.0, cols=[f'ch{k}' for k in range(num_cols)])


def test_round_on_data_with_jitter():
    data = dummy_data_with_jitter
    data.reset()
    node = Snap(rate=rate)
    node.i.data = data.next(6)
    node.update()

    expected_data = pd.DataFrame(
        [
            [0.185133, 0.541901, 0.872946, 0.732225, 0.806561],
            [0.658783, 0.692277, 0.849196, 0.249668, 0.489425],
            [0.221209, 0.987668, 0.944059, 0.039427, 0.705575],
            [0.925248, 0.180575, 0.567945, 0.915488, 0.033946],
            [0.69742, 0.297349, 0.924396, 0.971058, 0.944266],
            [0.474214, 0.862043, 0.844549, 0.3191, 0.828915]
        ],
        [
            pd.Timestamp('2018-01-01 00:00:00.000'),
            pd.Timestamp('2018-01-01 00:00:00.100'),
            pd.Timestamp('2018-01-01 00:00:00.200'),
            pd.Timestamp('2018-01-01 00:00:00.300'),
            pd.Timestamp('2018-01-01 00:00:00.400'),
            pd.Timestamp('2018-01-01 00:00:00.500'),
        ]
    )
    pd.testing.assert_frame_equal(expected_data, node.o.data)


def test_round_on_data_no_jitter():
    data = dummy_data_no_jitter
    data.reset()
    node = Snap(rate=rate)
    looper = Looper(data, node)
    dejittered_data, _ = looper.run(chunk_size=8)
    pd.testing.assert_frame_equal(dejittered_data, data._data)


def test_interpolate_on_data_no_jitter():
    data = dummy_data_no_jitter
    data.reset()
    interpolate = Interpolate(rate=rate, method='linear')
    looper = Looper(data, interpolate)
    dejittered_data, _ = looper.run(chunk_size=8)
    pd.testing.assert_frame_equal(dejittered_data, data._data.iloc[:-1])


def test_interpolate_on_linear_data():
    linear_data = pd.DataFrame(
        [
            [0.1],
            [0.2],
            [0.3],
            [0.4],
            [0.5],
            [0.6],
            [0.7],
            [0.8],
            [0.9],

        ],
        [
            pd.Timestamp('2018-01-01 00:00:00.000'),
            pd.Timestamp('2018-01-01 00:00:00.100'),
            pd.Timestamp('2018-01-01 00:00:00.200'),
            pd.Timestamp('2018-01-01 00:00:00.300'),
            pd.Timestamp('2018-01-01 00:00:00.400'),
            pd.Timestamp('2018-01-01 00:00:00.500'),
            pd.Timestamp('2018-01-01 00:00:00.600'),
            pd.Timestamp('2018-01-01 00:00:00.700'),
            pd.Timestamp('2018-01-01 00:00:00.800'),

        ],
        ['A']
    )
    linear_data_with_nan = linear_data.copy()
    linear_data_with_nan.iloc[[2, 3, 5]] = np.NaN

    interpolate = Interpolate(rate=10, method='linear')
    interpolate.i.data = linear_data_with_nan
    interpolate.update()
    pd.testing.assert_frame_equal(interpolate.o.data, linear_data.iloc[:-1])


def test_data_not_monotonic(caplog):
    data = pd.DataFrame(
        [[0, 0], [0, 0]],
        [pd.Timestamp('2018-01-01 00:00:00.100'), pd.Timestamp('2018-01-01 00:00:00.000')]
    )
    node = Interpolate(rate=rate)
    node.i.data = data
    node.update()
    assert caplog.record_tuples[0][2] == 'Data index should be strictly monotonic'
