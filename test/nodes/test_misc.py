"""Tests for miscellaneous nodes"""

import pytest
import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
from timeflux.core.registry import Registry
from timeflux.nodes.random import Random
from timeflux.nodes.hdf5 import Replay
from timeflux.helpers.clock import effective_rate

Registry.cycle_start = 0
Registry.rate = 1

def test_random():
    node = Random(2, 2, 2, 0, 10, seed=1)
    node.update()
    expected_index = np.empty(2, dtype='datetime64[us]')
    expected_data = [[5,8],[9,5]]
    expected = pd.DataFrame(expected_data, index=expected_index)
    assert_frame_equal(node.o.data, expected)

# TODO: make a generic test for replay
#def test_replay():
    # filename = pytest.path + '/data/replay.hdf5'
    # keys = ['/nexus/signal/nexus_signal_raw', '/unity/events/unity_events']
    # node = Replay(filename, keys, 40)
    # node.update()
    # rate = effective_rate(node.o_nexus_signal_nexus_signal_raw.data)
    # assert True