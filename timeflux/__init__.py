""" Timeflux """

__version__ = '0.2.0'

# Fix Ctrl-C handling in Windows
import os
os.environ['FOR_DISABLE_CONSOLE_CTRL_HANDLER'] = '1'
