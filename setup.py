""" Setup """

import re
from setuptools import setup, find_packages

with open('README.md', 'rb') as f:
    DESCRIPTION = f.read().decode('utf-8')

with open('timeflux/__init__.py') as f:
    VERSION = re.search('^__version__\s*=\s*\'(.*)\'', f.read(), re.M).group(1)

setup(
    name='Timeflux',
    packages=find_packages(),
    entry_points={
        'console_scripts': ['timeflux = timeflux.timeflux:main']
    },
    version=VERSION,
    description='Acquisition and real-time processing of biosignals.',
    long_description=DESCRIPTION,
    author='Pierre Clisson',
    author_email='contact@timeflux.io',
    url='https://timeflux.io',
)
