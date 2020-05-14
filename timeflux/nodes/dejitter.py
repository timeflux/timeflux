"""Dejittering nodes"""

import numpy as np
import pandas as pd
from timeflux.core.node import Node


class Snap(Node):
    """Snap time stamps to nearest occurring frequency.

    Attributes:
       i (Port): Default input, expects DataFrame and meta.
       o (Port): Default output, provides DataArray and meta.

    Args:
        rate (float|None): (optional) nominal sampling frequency of the data, to round
            the timestamps to (in Hz). If None, the rate will be get from the meta
            of the input port.
    """

    def __init__(self, rate=None):
        self._rate = rate

    def update(self):

        # copy the meta and the data
        self.o = self.i

        # if the rate has not been set in the constructor, get it from the meta
        if self._rate is None:
            self._rate = self.i.meta.get("rate")

        # When we have not received data, there is nothing to do
        if self._rate is None or self.i.data is None or self.i.data.empty:
            return

        # At this point, we are sure that we have some data to process
        self.o.data.index = self.o.data.index.round(str(1 / self._rate) + "S")
        self.o.meta["rate"] = self._rate


class Interpolate(Node):
    """Dejitter data with values interpolation.

    This nodes continuously buffers a small amount of data to allow for interpolating
    missing samples.
    The output data is resampled at a fixed rate.
    The interpolation is performed by Pandas methods.

    Attributes:
       i (Port): Default input, expects DataFrame and meta.
       o (Port): Default output, provides DataArray and meta.

    Args:
        rate (float|None): (optional) nominal sampling frequency of the data. If None,
        the rate will be get from the meta of the input port.
        method: interpolation method. See the pandas.DataFrame.interpolate documentation.
        n_min: minimum number of samples to perform the interpolation.
        n_max: number of samples to keep in the buffer.

    Notes:
        Computation cost mainly depends on the window size and the estimation is
        performed in the main thread. Hence, the user should be careful on the
        computation duration.
    """

    def __init__(self, rate=None, method="cubic", n_min=3, n_max=10):

        self._rate = rate
        if self._rate is not None:
            self._set_timedelta()

        self._method = method  # {‘linear’, ‘time’, ‘index’, ‘values’, ‘nearest’, ‘zero’, ‘slinear’, ‘quadratic’, ‘cubic’, ‘barycentric’, ‘krogh’, ‘polynomial’, ‘spline’, ‘piecewise_polynomial’, ‘from_derivatives’, ‘pchip’, ‘akima’}
        self._n_min = n_min
        self._n_max = n_max
        self._last_datetime = None

    def _set_timedelta(self):
        self._timespan = 1 / self._rate
        self._timedelta = pd.to_timedelta(
            self._timespan, "s"
        )  # sampling period of the interpolated signal

    def update(self):

        self.o.meta = self.i.meta

        # if the rate has not been set in the constructor, get it from the meta
        if self._rate is None:
            self._rate = self.i.meta.get("rate")
            self._set_timedelta()
        self.o.meta["rate"] = self._rate

        if self.i.data is None or self.i.data.empty:
            return

        # initialize the first datetime index.
        if self._last_datetime is None:
            self._last_datetime = self.i.data.index.round(str(self._timespan) + "S")[0]
            self._buffer = pd.DataFrame()
            self._times = np.arange(
                self._last_datetime,
                self.i.data.index[-1],
                self._timedelta,
                dtype="datetime64[us]",
            )
        else:
            self._times = np.arange(
                self._last_datetime + self._timedelta,
                self.i.data.index[-1],
                self._timedelta,
                dtype="datetime64[us]",
            )

        # interpolate
        self._interpolate()

    def _drop_duplicates(self, data):
        return data.loc[~data.index.duplicated(keep="first")]

    def _make_monotonic(self, data):
        return data[
            np.diff(pd.Index([self._last_datetime]).append(data.index))
            / np.timedelta64(1, "s")
            > 0
        ]

    def _interpolate(self):
        # interpolate current chunk
        self._buffer = self._buffer.append(
            self.i.data, sort=True
        )  # append last sample be able to interpolate

        if not self._buffer.index.is_monotonic:
            self.logger.warning("Data index should be strictly monotonic")
            self._buffer = self._make_monotonic(self._buffer)

        data_to_interpolate = self._buffer.append(
            pd.DataFrame(index=self._times), sort=True
        )
        data_to_interpolate = self._drop_duplicates(data_to_interpolate).sort_index()

        if (self._buffer.notnull().sum(axis=0) > self._n_min).all():
            self.o.data = (
                data_to_interpolate.interpolate(axis=0, method=self._method)
                .reindex(self._times)
                .loc[self._last_datetime :]
                .dropna(axis=0, how="any")
            )

            if not self.o.data.empty:
                self._last_datetime = self.o.data.index[-1]
                self._buffer = self._buffer.tail(self._n_max)
