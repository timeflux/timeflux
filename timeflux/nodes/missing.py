"""A node that handles missing data"""

import pandas as pd
from timeflux.core.node import Node


class DataFrameMissing(Node):
    """Handles missing data on an input port that usually supplies a dataframe.

    This node handles missing data of various kinds:
    - when the input is None, it can provide an empty dataframe;
    - when the input is None or an empty dataframe, it will acquire an index from the index port if available;
    - when the input contains NaNs, it can drop them;
    - when the input contains NaNs, it can fill them with an arbitrary value.

    All of these operations can be applied consecutively, so an input of None can be turned into a dataframe filled with some arbitrary value.

    When acquiring new index values from the index port, the new rows will have NaN values unless otherwise specified.
    Any existing index values are also preserved; to drop some or all of these, use an Interpolation node.

    Attributes:
        i (Port): Default data input, expects DataFrame and meta
        i_index (Port): (optional) Input to copy index from, expects DataFrame
        o (Port): Default output, provides DataFrame and meta.

    Args:
        create (bool): Whether to output an empty dataframe if input is None
        dropna (bool,string,dict): Whether to drop NaN values using Pandas dropna.
                         If dict, then the whole dict is supplied to pandas.DataFrame.dropna as **kwargs.
        fillna (bool,scalar,dict): Whether to replace NaN values using Pandas fillna.
                         If scalar then NaNs are replaced with that value.
                         If dict, then the whole dict is supplied to pandas.DataFrame.fillna as **kwargs.
    """

    def __init__(self, create=False, dropna=False, fillna=False, columns=None):

        self._create = create
        self._dropna = dropna
        self._fillna = fillna
        self._empty_dataframe = None
        if columns is not None:
            self._empty_dataframe = pd.DataFrame(columns=columns)

    def update(self):
        self.o = self.i

        if self.o.data is None:
            if self._create and not self._empty_dataframe is None:
                self.o.data = self._empty_dataframe
            else:
                return

        if self._create and self._empty_dataframe is None:
            self._empty_dataframe = pd.DataFrame(columns=self.i.data.columns)

        has_rows = self.o.data.shape[0] > 0
        if not (hasattr(self, 'i_index') and self.i_index.ready()):
            if not has_rows:
                # If we have an empty dataframe and no index data, there's nothing more we can do.
                return
        else:
            # Acquire indices from index port
            new_indices_df = pd.DataFrame(index=self.i_index.data.index, columns = self.o.data.columns)
            self.o.data = pd.concat([self.o.data, new_indices_df])
            if has_rows:
                # If the input data had rows of its own, then prefer those when they are duplicated.
                self.o.data = self.o.data[~self.o.data.index.duplicated(keep='first')]
                self.o.data = self.o.data.sort_index()

        if self._dropna is not False:
            args = self._dropna if type(self._dropna) is dict else {}
            self.o.data = self.o.data.dropna(**args)

        if self._fillna is not False:
            args = self._fillna if type(self._fillna) is dict else {'value': self._fillna}
            self.o.data = self.o.data.fillna(**args)





