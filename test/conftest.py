"""Test configuration"""

import os
import sys
import pytest

PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PATH  + '/../')

def pytest_configure(config):
    pytest.path = PATH
    config.addinivalue_line('filterwarnings', 'ignore:.*ABCs.*:DeprecationWarning')