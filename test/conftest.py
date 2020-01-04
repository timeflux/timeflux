"""Test configuration"""

import os
import sys
import importlib
import pytest
import numpy as np
import random as rand

def pytest_configure(config):
    pytest.path = os.path.dirname(os.path.abspath(__file__))

@pytest.fixture()
def random():
    """Initialize the random seeds for reproducibility."""
    rand.seed(0)
    np.random.seed(0)

@pytest.fixture()
def working_path(request):
    """Change the working directory to the test file path."""
    old = os.getcwd()
    new = os.path.dirname(request.fspath)
    os.chdir(new)
    yield
    os.chdir(old)