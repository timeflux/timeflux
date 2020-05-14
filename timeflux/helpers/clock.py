"""Time and rate helpers"""

import sys
import numpy as np
import pandas as pd
from time import time, perf_counter
from datetime import datetime


def now():
    """Return the current time as `np.datetime64['us']`."""
    # return pd.Timestamp(time(), unit='s')
    # return float_to_time(time())
    return np.datetime64(int(time() * 1e6), "us")


def float_to_time(timestamp):
    """Convert a `np.float64` to a `np.datetime64['us']`."""
    return np.datetime64(datetime.utcfromtimestamp(timestamp), "us")


def float_index_to_time_index(df):
    """Convert a dataframe float indices to `datetime64['us']` indices."""
    df.index = df.index.map(datetime.utcfromtimestamp)
    df.index = pd.to_datetime(df.index, unit="us", utc=True)
    return df


def time_to_float(timestamp):
    """Convert a `np.datetime64['us']` to a `np.float64`."""
    return timestamp.astype(np.float64) / 1e6


def min_time(unit="ns"):
    """Return the minimum datetime for this platform."""
    return np.datetime64(-int(sys.maxsize), unit)


def max_time(unit="ns"):
    """Return the maximum datetime for this platform."""
    return np.datetime64(int(sys.maxsize), unit)


def effective_rate(df):
    """A simple method to compute the effective rate."""
    if df.shape[0] < 2:
        return None
    rate = df.shape[0] / (df.index[-1] - df.index[0]).total_seconds()
    return rate


def absolute_offset():
    """Return the offset between the UTC timestamp and a precision timer such as the LSL precision clock."""
    return pd.Timestamp(time(), unit="s") - pd.Timestamp(perf_counter(), unit="s")


def time_range(start, stop, num):
    """Return ``num`` evenly spaced timestamps between ``start`` and ``stop`` (`np.datetime64`)."""
    return np.linspace(
        start.astype(np.uint64),
        stop.astype(np.uint64),
        num,
        False,
        dtype="datetime64[us]",
    )
