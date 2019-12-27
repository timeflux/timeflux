"""Test configuration"""

import os
import sys
import pytest
import numpy as np
import random as rand

PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(PATH, '../'))

def pytest_configure(config):
    pytest.path = PATH
    config.addinivalue_line('filterwarnings', 'ignore:.*ABCs.*:DeprecationWarning')

@pytest.fixture(scope='module')
def random():
    rand.seed(0)
    np.random.seed(0)
