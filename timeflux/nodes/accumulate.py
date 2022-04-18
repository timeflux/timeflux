"""Accumulation nodes that either, stack, append or, concatenate data after a gate"""

import pandas as pd
import xarray as xr
from timeflux.core.node import Node


class AppendDataFrame(Node):
    """Accumulates and appends data of type DataFrame after a gate.

    This node should be plugged after a Gate. As long as it receives data,
    it appends them to an internal buffer. When it receives a meta with key
    `gate_status` set to `closed`, it releases the accumulated data and empty the
    buffer.

    Attributes:
        i (Port): Default data input, expects DataFrame and meta
        o (Port): Default output, provides DataFrame

    Args:
        **kwargs: key word arguments to pass to pandas.DataFrame.concat method.

    """

    def __init__(self, meta_keys=None, **kwargs):

        super().__init__()
        self._meta_keys = meta_keys
        self._kwargs = kwargs
        self._reset()

    def _reset(self):
        self._data = pd.DataFrame()
        self._meta = []

    def _release(self):
        self.logger.info(
            f"AppendDataFrame is releasing {len(self._data)} " f"accumulated rows."
        )
        self.o.data = self._data
        if self._meta_keys is None:
            self.o.meta = {"accumulate": self._meta}
        else:
            self.o.meta = {key: [] for key in self._meta_keys}
            for meta_key in self._meta_keys:
                for meta in self._meta:
                    self.o.meta[meta_key] += meta.get(meta_key, [])

    def update(self):

        gate_status = self.i.meta.get("gate_status")

        if self.i.ready():
            # update the meta
            self._meta.append(self.i.meta)

            # append the data
            self._data = pd.concat([self._data, self.i.data], **self._kwargs)

        # if gate is close, release the data and reset the buffer
        if gate_status == "closed" and not self._data.empty:
            self._release()
            self._reset()


class AppendDataArray(Node):
    """Accumulates and appends data of type XArray after a gate.

    This node should be plugged after a Gate. As long as it receives DataArrays,
    it appends them to a buffer list. When it receives a meta with key
    `gate_status` set to `closed`, it concatenates the list of accumulated DataArray,
    releases it and empty the buffer list.

    Attributes:
        i (Port): Default data input, expects DataArray and meta
        o (Port): Default output, provides DataArray

    Args:
        dim: Name of the dimension to concatenate along.
        **kwargs: key word arguments to pass to xarray.concat method.

    """

    def __init__(self, dim, meta_keys=None, **kwargs):

        super().__init__()
        self._dim = dim
        self._meta_keys = meta_keys
        self._kwargs = kwargs
        self._reset()

    def _reset(self):
        self._data_list = []
        self._meta = []

    def _release(self):
        self.logger.info(
            f"AppendDataArray is releasing {len(self._data_list)} "
            f"accumulated data chunks."
        )
        self.o.data = xr.concat(self._data_list, self._dim, **self._kwargs)
        if self._meta_keys is None:
            self.o.meta = {"accumulate": self._meta}
        else:
            self.o.meta = {key: [] for key in self._meta_keys}
            for meta_key in self._meta_keys:
                for meta in self._meta:
                    self.o.meta[meta_key] += meta.get(meta_key, [])

    def update(self):

        gate_status = self.i.meta.get("gate_status")

        # append the data
        if self.i.ready():
            self._data_list.append(self.i.data)
            # update the meta
            self._meta.append(self.i.meta)

        # if gate is close, release the data and reset the buffer
        if gate_status == "closed" and self._data_list:
            self._release()
            self._reset()
