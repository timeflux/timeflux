""" Timeflux """

# Fix Ctrl-C handling in Windows
import os
os.environ['FOR_DISABLE_CONSOLE_CTRL_HANDLER'] = '1'

# Versinoning
from pbr.version import VersionInfo
__version__ = VersionInfo(__name__).release_string()
