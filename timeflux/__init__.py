""" Timeflux """

# Fix Ctrl-C handling in Windows
import os
os.environ['FOR_DISABLE_CONSOLE_CTRL_HANDLER'] = '1'

# Versinoning
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
