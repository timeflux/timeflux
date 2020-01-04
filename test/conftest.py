"""Test configuration"""

import os
import sys
import importlib
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

@pytest.fixture(scope='module')
def dummy_module(request):
    file = request.fspath
    name = 'test'
    spec = importlib.util.spec_from_file_location(name, file)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
