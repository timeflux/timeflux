"""Accumulation nodes that either, stack, append or, concatenate data after a gate"""

import pandas as pd
import xarray as xr
from timeflux.core.node import Node


class Append_DataFrame(Node):

    def __init__(self, **kwargs):
        """
        """
        super().__init__()

        self._kwargs = kwargs
        self._reset()

    def _reset(self):
        self._data = pd.DataFrame()
        self._meta = {}

    def _release(self):
        self.o.data = self._data
        self.o.meta = self._meta

    def update(self):

        gate_status = self.i.meta.get('gate_status')

        # When we have not received data, there is nothing to do
        if self.i.data is None or self.i.data.empty:
            return
        # At this point, we are sure that we have some data to process

        # update the meta
        self._meta.update(self.i.meta)

        # append the data
        self._data = self._data.append(self.i.data, **self._kwargs)

        # if gate is close, release the data and reset the buffer
        if gate_status == 'closed':
            self._release()
            self._reset()


class Append_DataArray(Node):

    def __init__(self, dim, **kwargs):
        """
        """
        super().__init__()
        self._dim = dim
        self._kwargs = kwargs
        self._reset()

    def _reset(self):
        self._data_list = []
        self._meta = {}

    def _release(self):
        self.o.data = xr.concat(self._data_list, self._dim, **self._kwargs)
        self.o.meta = self._meta

    def update(self):

        gate_status = self.i.meta.get('gate_status')

        # When we have not received data, there is nothing to do
        if self.i.data is None or self.i.data.empty:
            return
        # At this point, we are sure that we have some data to process

        # update the meta
        self._meta.update(self.i.meta)

        # append the data
        self._data_list.append(self.i.data)

        # if gate is close, release the data and reset the buffer
        if gate_status == 'close':
            self._release()
            self._reset()
