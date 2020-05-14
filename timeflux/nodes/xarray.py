"""XArray

This module contains nodes to handle XArray data.

"""

from timeflux.core.node import Node


class Transpose(Node):
    """Transpose dimensions of a DataArray.

    This node reorders the dimensions of a DataArray object to ``dims``.

    Attributes:
        i (Port): default data input, expects DataArray.
        o (Port): default output, provides DataArray.

    Args:
        dims (list, None): By default, reverse the dimensions. Otherwise, reorder the dimensions to this order.

    """

    def __init__(self, dims):

        self._dims = dims

    def update(self):

        if not self.i.ready():
            return

        self.o.meta = self.i.meta
        self.o.data = self.i.data.transpose(*self._dims)


class ToDataFrame(Node):
    """Convert XArray to DataFrame.

    This node converts a XArray into a flat DataFrame with simple index given
    by dimension in ``index_dim`` and eventually MultiIndex columns
    (`nb_levels = n_dim -1`, where n_dim is the number of dimensions of the XArray in input ).

    Attributes:
        i (Port): default data input, expects DataArray.
        o (Port): default output, provides DataFrame.


    Args:
        index_dim (str, `time`): Name of the dimension to set the index of the DataFrame.
    """

    def __init__(self, index_dim="time"):

        self._index_dim = index_dim
        self._indexes = None

    def _set_indexes(self, data):
        self._indexes_to_unstack = [
            index for index in list(data.indexes.keys()) if index != self._index_dim
        ]

    def update(self):

        if not self.i.ready():
            return

        self.o.meta = self.i.meta

        if self._indexes_to_unstack is None:
            self._set_indexes(self.i.data)
        self.o.data = self.i.data.to_dataframe("data").unstack(self._indexes_to_unstack)
        self.o.data.columns = self.o.data.columns.droplevel(level=0)
