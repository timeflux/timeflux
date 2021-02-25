"""A set of tools to facilitate code testing"""

import numpy as np
import pandas as pd
import xarray as xr


class DummyData:
    """Generate dummy data."""

    def __init__(
        self,
        num_rows=1000,
        num_cols=5,
        cols=None,
        rate=10,
        jitter=0.05,
        start_date="2018-01-01",
        seed=42,
        round=6,
    ):
        """
        Initialize the dataframe.

        Args:
            num_rows (int): Number of rows
            num_cols (int): Number of columns
            cols (list): List of column names
            rate (float): Frequency, in Hertz
            jitter (float): Amount of jitter, relative to rate
            start_date (string): Start date
            seed (int): Seed for random number generation
            round (int): Number of decimals for random numbers
        """

        np.random.seed(seed)

        frequency = 1 / rate

        indices = pd.date_range(
            start=start_date, periods=num_rows, freq=pd.DateOffset(seconds=frequency)
        )

        jitter = frequency * jitter
        deltas = pd.to_timedelta(np.random.uniform(-jitter, jitter, num_rows), unit="s")
        indices = indices + deltas

        if cols is not None:
            num_cols = len(cols)

        rows = np.random.rand(num_rows, num_cols).round(round)

        self._data = pd.DataFrame(rows, indices)
        if cols is not None:
            self._data.columns = cols
        self._cursor = 0

    def next(self, num_rows=10):
        """
        Get the next chunk of data.

        Args:
            num_rows (int): Number of rows to fetch
        """

        start = self._cursor
        stop = start + num_rows
        self._cursor += num_rows
        return self._data[start:stop]

    def reset(self):
        """
        Reset the cursor.
        """

        self._cursor = 0


class DummyXArray:
    """Generate dummy data of type XArray."""

    def __init__(
        self,
        num_time=1000,
        num_space=5,
        rate=10,
        jitter=0.05,
        start_date="2018-01-01",
        seed=42,
        round=6,
    ):
        """
        Initialize the dataframe.

        Args:
            num_time (int): Number of rows
            num_space (int): Number of columns
            rate (float): Frequency, in Hertz
            jitter (float): Amount of jitter, relative to rate
            start_date (string): Start date
            seed (int): Seed for random number generation
            round (int): Number of decimals for random numbers
        """

        np.random.seed(seed)
        frequency = 1 / rate

        times = pd.date_range(
            start=start_date, periods=num_time, freq=pd.DateOffset(seconds=frequency)
        )

        jitter = frequency * jitter
        deltas = pd.to_timedelta(np.random.uniform(-jitter, jitter, num_time), unit="s")
        times = times + deltas
        locs = np.arange(num_space)
        data = np.random.rand(num_time, num_space).round(round)
        self._data = xr.DataArray(data, coords=[times, locs], dims=["time", "space"])
        self._cursor = 0

    def next(self, num_rows=10):
        """
        Get the next chunk of data.

        Args:
            num_rows (int): Number of rows to fetch
        """

        start = self._cursor
        stop = start + num_rows
        self._cursor += num_rows
        return self._data.isel({"time": np.arange(start, stop)})

    def reset(self):
        """
        Reset the cursor.
        """

        self._cursor = 0


class ReadData:
    """Generate custom data."""

    def __init__(self, data):
        """
        Initialize the dataframe.

        Args:
            data (DataFrame): custom data to stream.
        """

        self._data = data
        self._cursor = 0

    def next(self, num_rows=10):
        """
        Get the next chunk of data.

        Args:
            num_rows (int): Number of rows to fetch
        """

        start = self._cursor
        stop = start + num_rows
        self._cursor += num_rows
        return self._data.iloc[start:stop]

    def reset(self):
        """
        Reset the cursor.
        """

        self._cursor = 0


class Looper:
    """Mimics the scheduler behavior to allow testing the output of a node offline."""

    def __init__(self, generator, node, input_port="i", output_port="o"):
        """Initialize the helper
        :param generator (Node): timeflux node to test
        :param data (Object): data generator object with a method `next` and `reset`
        """
        self._generator = generator
        self._node = node
        self._input_port = input_port
        self._output_port = output_port

    def run(self, chunk_size=None):
        """Loop across chunks of a generator, update the node and return data and meta.
        :param chunk_size (int): number of samples per chunk
        :return:
        output_data (DataFrame): concatenated output data
        output_meta: list of meta
        """

        chunk_size = chunk_size or len(self._generator._data)

        # mimic the scheduler
        end_of_data = False
        output_data = []
        output_meta = []
        while not end_of_data:
            self._node.clear()
            chunk = self._generator.next(chunk_size)
            i = getattr(self._node, self._input_port)
            i.data = chunk.copy()
            self._node.update()
            o = getattr(self._node, self._output_port)
            output_data.append(o.data)
            output_meta.append(o.meta)
            end_of_data = chunk.empty
        output_data = pd.concat(output_data)
        return output_data, output_meta
