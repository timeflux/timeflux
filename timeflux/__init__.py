""" Timeflux """

# Fix Ctrl-C handling in Windows
import os

os.environ["FOR_DISABLE_CONSOLE_CTRL_HANDLER"] = "1"

# Versioning
try:
    from setuptools_scm import get_version

    __version__ = get_version(root="..", relative_to=__file__)
except:
    try:
        from .version import version

        __version__ = version
    except:
        __version__ = "0.0.0"
