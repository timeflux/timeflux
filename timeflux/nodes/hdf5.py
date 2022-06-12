"""timeflux.nodes.hdf5: HDF5 nodes"""

import pandas as pd
import timeflux.helpers.clock as clock
import sys
import os
import time
from timeflux.core.exceptions import WorkerInterrupt, WorkerLoadError
from timeflux.core.node import Node

# Ignore the "object name is not a valid Python identifier" message
import warnings
from tables.exceptions import NaturalNameWarning

warnings.simplefilter("ignore", NaturalNameWarning)


class Replay(Node):
    """Replay a HDF5 file."""

    def __init__(self, filename, keys, speed=1, timespan=None, resync=True, start=0):
        """
        Initialize.

        Parameters
        ----------
        filename : string
            The path to the HDF5 file.
        keys: list
            The list of keys to replay.
        speed: float
            The speed at which the data must be replayed. 1 means real-time.
            Default: 1
        timespan: float
            The timespan of each chunk, in seconds.
            If not None, will take precedence over the `speed` parameter
            Default: None
        resync: boolean
            If False, timestamps will not be resync'ed to current time
            Default: True
        start: float
            Start directly at the given time offset, in seconds
            Default: 0
        """

        # Load store
        try:
            self._store = pd.HDFStore(self._find_path(filename), mode="r")
        except IOError as e:
            raise WorkerInterrupt(e)

        # Init
        self._sources = {}
        self._start = pd.Timestamp.max
        self._stop = pd.Timestamp.min
        self._speed = speed
        self._timespan = None if not timespan else pd.Timedelta(f"{timespan}s")
        self._resync = resync

        for key in keys:
            try:
                # Check format
                if not self._store.get_storer(key).is_table:
                    self.logger.warning("%s: Fixed format. Will be skipped.", key)
                    continue
                # Get first index
                first = self._store.select(key, start=0, stop=1).index[0]
                # Get last index
                nrows = self._store.get_storer(key).nrows
                last = self._store.select(key, start=nrows - 1, stop=nrows).index[0]
                # Check index type
                if type(first) != pd.Timestamp:
                    self.logger.warning("%s: Invalid index. Will be skipped.", key)
                    continue
                # Find lowest and highest indices across stores
                if first < self._start:
                    self._start = first
                if last > self._stop:
                    self._stop = last
                # Extract meta
                if self._store.get_node(key)._v_attrs.__contains__("meta"):
                    meta = self._store.get_node(key)._v_attrs["meta"]
                else:
                    meta = {}
                # Set output port name, port will be created dynamically
                name = "o" + key.replace("/", "_")
                # Update sources
                self._sources[key] = {
                    "start": first,
                    "stop": last,
                    "nrows": nrows,
                    "name": name,
                    "meta": meta,
                }
            except KeyError:
                self.logger.warning("%s: Key not found.", key)

        # Current time
        now = clock.now()

        # Starting timestamp
        self._start += pd.Timedelta(f"{start}s")

        # Time offset
        self._offset = pd.Timestamp(now) - self._start

        # Current query time
        self._current = self._start

        # Last update
        self._last = now

    def update(self):

        if self._current > self._stop:
            raise WorkerInterrupt("No more data.")

        min = self._current

        if self._timespan:
            max = min + self._timespan
        else:
            now = clock.now()
            ellapsed = now - self._last
            max = min + ellapsed * self._speed
            self._last = now

        for key, source in self._sources.items():

            # Select data
            data = self._store.select(key, "index >= min & index < max")

            # Add offset
            if self._resync:
                data.index += self._offset

            # Update port
            getattr(self, source["name"]).data = data
            getattr(self, source["name"]).meta = source["meta"]

        self._current = max

    def terminate(self):
        self._store.close()

    def _find_path(self, path):
        path = os.path.normpath(path)
        if os.path.isabs(path):
            if os.path.isfile(path):
                return path
        else:
            for base in sys.path:
                full_path = os.path.join(base, path)
                if os.path.isfile(full_path):
                    return full_path
        raise WorkerLoadError(f"File `{path}` could not be found in the search path.")


class Save(Node):
    """Save to HDF5."""

    def __init__(
        self, filename=None, path="/tmp", complib="zlib", complevel=3, min_itemsize=None
    ):
        """
        Initialize.

        Parameters
        ----------
        filename: string
            Name of the file (inside the path set by parameter). If not set,
            an auto-generated filename is used.
        path : string
            The directory where the HDF5 file will be written.
            Default: "/tmp"
        complib : string
            The compression lib to be used.
            see: https://www.pytables.org/usersguide/libref/helper_classes.html
            Default: "zlib"
        complevel : int
            The compression level. A value of 0 disables compression.
            Default: 3
            see: https://www.pytables.org/usersguide/libref/helper_classes.html
        min_itemsize : int
            The string columns size
            Default: None
            see: https://pandas.pydata.org/pandas-docs/stable/generated/pandas.HDFStore.append.html
            see: http://pandas.pydata.org/pandas-docs/stable/io.html#string-columns

        """
        os.makedirs(path, exist_ok=True)
        if filename is None:
            filename = os.path.join(
                path, time.strftime("%Y%m%d-%H%M%S.hdf5", time.gmtime())
            )
        else:
            filename = os.path.join(path, filename)
        self.logger.info("Saving to %s", filename)
        self._store = pd.HDFStore(filename, complib=complib, complevel=complevel)
        self.min_itemsize = min_itemsize

    def update(self):
        if self.ports is not None:
            for name, port in self.ports.items():
                if not name.startswith("i"):
                    continue
                key = "/" + name[2:].replace("_", "/")
                if port.data is not None:
                    if isinstance(port.data, pd.DataFrame):
                        port.data.index.freq = None
                    self._store.append(key, port.data, min_itemsize=self.min_itemsize)
                if port.meta is not None and port.meta:
                    # Note: not none and not an empty dict, because this operation
                    #       overwrites previous metadata and an empty dict would
                    #       just remove any previous change
                    node = self._store.get_node(key)
                    if node:
                        self._store.get_node(key)._v_attrs["meta"] = port.meta

    def terminate(self):
        try:
            self._store.close()
        except Exception:
            # Just in case
            pass
