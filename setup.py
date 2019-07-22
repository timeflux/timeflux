""" Setup """

import versioneer
from setuptools import setup, find_packages

with open('README.md', 'rb') as f:
    DESCRIPTION = f.read().decode('utf-8')

DEPENDENCIES = [
    'networkx',
    'PyYAML',
    'numpy',
    'pandas',
    'xarray',
    'bottleneck',
    'scipy',
    'pyzmq',
    'coloredlogs',
    'tables',
    'pylsl',
    'python-osc',
    'python-dotenv',
    'jsonschema'
]

setup(
    name='timeflux',
    packages=find_packages(),
    package_data={
        'timeflux': ['schema/app.json']
    },
    entry_points={
        'console_scripts': ['timeflux = timeflux.timeflux:main']
    },
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Timeflux: acquisition and real-time processing of biosignals.',
    long_description=DESCRIPTION,
    author='Pierre Clisson',
    author_email='contact@timeflux.io',
    url='https://timeflux.io',
    install_requires=DEPENDENCIES
)
