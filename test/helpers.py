import pandas as pd
import numpy as np


class DummyData():
    """Generate dummy data."""

    def __init__(self,
                 num_rows=1000,
                 num_cols=5,
                 rate=10,
                 jitter=.05,
                 start_date='2018-01-01',
                 seed=42,
                 round=6):
        """
        Initialize the dataframe.

        Args:
            num_rows (int): Number of rows
            num_cols (int): Number of columns
            rate (float): Frequency, in Hertz
            jitter (float): Amount of jitter, relative to rate
            start_date (string): Start date
            seed (int): Seed for random number generation
            round (int): Number of decimals for random numbers
        """

        np.random.seed(seed)

        frequency = 1 / rate

        indices = pd.date_range(
            start=start_date,
            periods=num_rows,
            freq=pd.DateOffset(seconds=frequency))

        jitter = frequency * jitter
        deltas = pd.to_timedelta(
            np.random.uniform(-jitter, jitter, num_rows), unit='s')
        indices = indices + deltas

        rows = np.random.rand(num_rows, num_cols).round(round)

        self._data = pd.DataFrame(rows, indices)
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
