"""timeflux.nodes.hdf5: HDF5 nodes"""

import pandas as pd
import numpy as np
import timeflux.helpers.clock as clock
import logging
import os
import time
from timeflux.core.exceptions import WorkerInterrupt
from timeflux.core.node import Node
from timeflux.core.io import Port

class Replay(Node):

    """Replay a HDF5 file."""

    def __init__(self, filename, keys, timespan=.04):
        """
        Initialize.

        Parameters
        ----------
        filename : string
            The path to the HDF5 file.
        keys: list
            The list of keys to replay.
        timespan: float
            The timespan of each chunk, in seconds.
        """

        # Load store
        try:
            self._store = pd.HDFStore(filename, mode='r')
        except IOError as e:
            raise WorkerInterrupt(e)

        # Init
        self._sources = {}
        self._start = pd.Timestamp.max
        self._stop = pd.Timestamp.min
        self._timespan = pd.Timedelta(f'{timespan}s')

        for key in keys:
            try:
                # Check format
                if not self._store.get_storer(key).is_table:
                    logging.warning('%s: Fixed format. Will be skipped.', key)
                    continue
                # Get first index
                first = self._store.select(key, start=0, stop=1).index[0]
                # Get last index
                nrows = self._store.get_storer(key).nrows
                last = self._store.select(key, start=nrows-1, stop=nrows).index[0]
                # Check index type
                if type(first) != pd.Timestamp:
                    logging.warning('%s: Invalid index. Will be skipped.', key)
                    continue
                # Find lowest and highest indices across stores
                if first < self._start:
                    self._start = first
                if last > self._stop:
                    self._stop = last
                # Set output port name, port will be created dynamically
                name = 'o' + key.replace('/', '_')
                # Update sources
                self._sources[key] = {
                    'start': first,
                    'stop': last,
                    'nrows': nrows,
                    'name': name
                }
            except KeyError:
                logging.warning('%s: Key not found.', key)

        # Time offset
        self._offset = pd.Timestamp(clock.now()) - self._start

        # Current query time
        self._current = self._start


    def update(self):

        if self._current > self._stop:
            raise WorkerInterrupt('No more data.')

        min = self._current
        max = min + self._timespan

        for key, source in self._sources.items():

            # Select data
            data = self._store.select(key, 'index >= min & index < max')

            # Add offset
            data.index += self._offset

            # Update port
            getattr(self, source['name']).data = data

        self._current = max


class Save(Node):

    """Save to HDF5."""

    def __init__(self, path='/tmp', complib='zlib', complevel=9, min_itemsize=None):
        """
        Initialize.

        Parameters
        ----------
        path : string
            The directory where the HDF5 file will be written.
        complib : string
            The compression lib to be used.
            see: https://www.pytables.org/usersguide/libref/helper_classes.html
        complevel : int
            The compression level. A value of 0 disables compression.
            see: https://www.pytables.org/usersguide/libref/helper_classes.html
        min_itemsize : int | dict
            The string columns size
            see: https://pandas.pydata.org/pandas-docs/stable/generated/pandas.HDFStore.append.html
            see: http://pandas.pydata.org/pandas-docs/stable/io.html#string-columns

        """
        fname = os.path.join(path, time.strftime('%Y%m%d-%H%M%S.hdf5', time.gmtime()))
        logging.info('Saving to %s', fname)
        self._store = pd.HDFStore(fname, complib=complib, complevel=complevel)
        self.min_itemsize = min_itemsize
        print(min_itemsize)


    def update(self):
        if self.ports is not None:
            for name, port in self.ports.items():
                if port.data is not None:
                    if name.startswith('i'):
                        key = '/' + name[2:].replace('_', '/')
                        self._store.append(key, port.data, min_itemsize=self.min_itemsize)
