
"""Conversion

This module contains nodes to convert Xarray to Pandas and vice versa.

"""

from timeflux.core.node import Node
import xarray as xr


class XRTranspose(Node):
    """Transpose dimensions of a DaraArray.

    This node reorders the dimensions of a DataArray object to ``dims``.

    Attributes:
        i (Port): default data input, expects DataArray.
        o (Port): default output, provides DataArray.

    """

    def __init__(self, dims):
        """
        Args:
            dims (list, None): By default, reverse the dimensions. Otherwise, reorder the dimensions to this order.

        """

        self._dims = dims

    def update(self):

        self.o.meta = self.i.meta
        if self.i.data is not None:
            if type(self.i.data) == xr.core.dataarray.DataArray:
                try:
                    self.o.data = self.i.data.transpose(*self._dims)
                except ValueError as msg:
                    self.logger.error(msg)


class XR_to_DF(Node):
    """Convert XArray to DataFrame.

    This node converts a XArray into a flat DataFrame with simple index given by dimension in ``index_dim`` and eventually MultiIndex columns (`nb_levels = n_dim -1`, where n_dim is the number of dimensions of the XArray in input ).

    Attributes:
        i (Port): default data input, expects DataArray.
        o (Port): default output, provides DataFrame.

    """

    def __init__(self, index_dim="time"):
        """
        Args:
            index_dim (str, `time`): Name of the dimension to set the index of the DataFrame.
        """

        self._index_dim = index_dim
        self._indexes = None

    def _set_indexes(self, data):

        self._indexes_to_unstack = [index for index in list(data.indexes.keys()) if index != self._index_dim]

    def update(self):

        self.o.meta = self.i.meta
        if self.i.data is not None:
            if type(self.i.data) == xr.core.dataarray.DataArray:
                if self._indexes_to_unstack is None: self._set_indexes(self.i.data)
                self.o.data = self.i.data.to_dataframe("data").unstack(self._indexes_to_unstack)
                self.o.data.columns = self.o.data.columns.droplevel(level=0)
