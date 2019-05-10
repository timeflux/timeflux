"""timeflux.nodes.hdf5: HDF5 nodes"""

import pandas as pd
import numpy as np
import timeflux.helpers.clock as clock
import os
import time
from timeflux.core.exceptions import WorkerInterrupt
from timeflux.core.node import Node
from timeflux.core.io import Port

class Replay(Node):

    """Replay a HDF5 file."""

    def __init__(self, filename, keys, timespan=.04, resync=True):
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
        resync: boolean
            If False, timestamps will not be resync'ed to current time
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
        self._resync = resync

        for key in keys:
            try:
                # Check format
                if not self._store.get_storer(key).is_table:
                    self.logger.warning('%s: Fixed format. Will be skipped.', key)
                    continue
                # Get first index
                first = self._store.select(key, start=0, stop=1).index[0]
                # Get last index
                nrows = self._store.get_storer(key).nrows
                last = self._store.select(key, start=nrows-1, stop=nrows).index[0]
                # Check index type
                if type(first) != pd.Timestamp:
                    self.logger.warning('%s: Invalid index. Will be skipped.', key)
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
                self.logger.warning('%s: Key not found.', key)

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
            if self._resync:
                data.index += self._offset

            # Update port
            getattr(self, source['name']).data = data

        self._current = max

    def terminate(self):
        self._store.close()


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
        os.makedirs(path, exist_ok=True)
        fname = os.path.join(path, time.strftime('%Y%m%d-%H%M%S.hdf5', time.gmtime()))
        self.logger.info('Saving to %s', fname)
        self._store = pd.HDFStore(fname, complib=complib, complevel=complevel)
        self.min_itemsize = min_itemsize


    def update(self):
        if self.ports is not None:
            for name, port in self.ports.items():
                if not name.startswith('i'):
                    continue
                key = '/' + name[2:].replace('_', '/')
                if port.data is not None:
                    self._store.append(key, port.data, min_itemsize=self.min_itemsize)
                if port.meta is not None and port.meta:
                    # Note: not none and not an empty dict, because this operation
                    #       overwrites previous metadata and an empty dict would
                    #       just remove any previous change
                    self.logger.info('Saving meta for %s', key)
                    self._store.get_node(key)._v_attrs['timeflux_meta'] = port.meta


    def terminate(self):
        self._store.close()
