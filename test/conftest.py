"""Test configuration"""

import os
import sys
import pytest

PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PATH  + '/../')

def pytest_namespace():
    return {'path': PATH}

def pytest_configure(config):
    config.addinivalue_line('filterwarnings', 'ignore:.*ABCs.*:DeprecationWarning')