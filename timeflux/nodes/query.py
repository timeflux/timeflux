
from timeflux.core.node import Node
import pandas as pd
import numpy as np

import logging
from ..helpers.xarray import proper_unstack
class SelectRange(Node):
    """Select a subset of the given data along vertical (index) or horizontal (columns) axis.

    Attributes:
        i (Port): default data input, expects DataFrame with eventually MultiIndex.
        o (Port): default output, provides DataFrame with eventually MultiIndex.


    Example:

        In this example, we have an input DataFrame with multi level columns and we want to select data with index from level of name `second` in range `[1,1.5]`.
        We set:

        * ``ranges`` = `{"second": [1, 1.5]}`
        * ``axis`` = `1`
        * ``inclusive`` = `True`

        If the data received on port ``i`` is: ::

            first                                 A                        ...            B
            second                              1.3       1.6       1.9                 1.3       1.6       1.9
            2017-12-31 23:59:59.998745401  0.185133  0.541901  0.806561    ...     0.732225  0.806561  0.658783
            2018-01-01 00:00:00.104507143  0.692277  0.849196  0.987668    ...     0.489425  0.221209  0.987668
            2018-01-01 00:00:00.202319939  0.944059  0.039427  0.567945    ...     0.925248  0.180575  0.567945

        The data provided on port ``o`` will be: ::

            first                                 A         B
            second                              1.3       1.3
            2017-12-31 23:59:59.998745401  0.185133  0.732225
            2018-01-01 00:00:00.104507143  0.692277  0.489425
            2018-01-01 00:00:00.202319939  0.944059  0.925248


    """
    def __init__(self, ranges,  axis = 0, inclusive = False) :

        """
            Args:
             ranges (dict): Dict with keys are level names and values are selection ranges.
             axis (int): If 0, the level concerns row index, if 1, columns index (`0` or `1`). Default: `0`.
             inclusive (bool) : Whether the boundaries are strict or included. Default: `False`.

          """

        self._ranges = ranges  # list of ranges per level
        self._inclusive = inclusive #include boundaries.
        self._axis = axis

    def update(self):

        self.o = self.i
        if self.i.data is not None:
            if not self.i.data.empty:
                if self._axis == 0:
                    if self._inclusive:
                        cond_list = [(self.i.data.index.get_level_values(l) >= r[0]) & (self.i.data.index.get_level_values(l) <= r[1]) for l, r in (self._ranges).items() if r is not None]
                    else:
                        cond_list = [(self.i.data.index.get_level_values(l) > r[0]) & (self.i.data.index.get_level_values(l) < r[1]) for l, r in (self._ranges).items() if r is not None]

                    self.o.data = self.i.data[np.logical_and.reduce(cond_list)]

                else : # axis == 1
                    if self._inclusive:
                        cond_list = [(self.i.data.columns.get_level_values(l) >= r[0]) & (self.i.data.columns.get_level_values(l) <= r[1]) for l, r in (self._ranges).items() if r is not None]
                    else:
                        cond_list = [(self.i.data.columns.get_level_values(l) > r[0]) & (self.i.data.columns.get_level_values(l) < r[1]) for l, r in (self._ranges).items() if r is not None]
                    self.o.data = self.i.data.T[np.logical_and.reduce(cond_list)].T


class XsQuery(Node):
    """Returns a cross-section (row(s) or column(s)) from the data.

    Attributes:
        i (Port): default input, expects DataFrame with eventually MultiIndex.
        o (Port): default output, provides DataFrame with eventually MultiIndex.

    Example:

        In this example, we have an input DataFrame with multi level columns and we want to select cross section between `B` from level of name `first` and `1` from level of name `second`.
        We set:

            * ``key`` = `("B", 1)`
            * ``axis`` = `1`
            * ``level`` = `["first", "second"]`
            * ``drop_level`` = `False`

    If the data received on port ``i`` is: ::

        first                                 A              ...            B
        second                                1         2    ...            1         2
        2017-12-31 23:59:59.998745401  0.185133  0.541901    ...     0.297349  0.806561
        2018-01-01 00:00:00.104507143  0.692277  0.849196    ...     0.844549  0.221209
        2018-01-01 00:00:00.202319939  0.944059  0.039427    ...     0.120567  0.180575

    The data provided on port ``o`` will be: ::

        first                                 B
        second                                1
        2018-01-01 00:00:00.300986584  0.297349
        2018-01-01 00:00:00.396560186  0.844549
        2018-01-01 00:00:00.496559945  0.120567

    References:

        See the documentation of `pandas.DataFrame.xs <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.xs.html>`_  .

    """


    def __init__(self, key, axis=0, level=None, drop_level=False ) :

        """
        Args:
           key (str|tuple): Some label contained in the index, or partially in a MultiIndex index.
           axis (int): Axis to retrieve cross-section on (`0` or `1`). Default: `0`.
           level (str|int|tuple) : In case of a key partially contained in a MultiIndex, indicates which levels are used. Levels can be referred by label or position.
           drop_level (bool) : If False, returns DataFrame with same level. Default: `False`.

        """

        self._key = key
        self._axis = axis
        self._level = level
        self._drop_level = drop_level

    def update(self):
        self.o = self.i
        self.o.data = self.i.data.xs(key=self._key, axis=self._axis, level=self._level, drop_level=self._drop_level)

class LocQuery(Node):
    """Slices DataFrame on group of rows and columns by label(s)

    Attributes:
        i (Port): default data input, expects DataFrame.
        o (Port): default output, provides DataFrame.

    Example:

        In this example, we have an input DataFrame with 5 columns `[A, B, C, D, E]` and we want to select columns A and E.
        We set:

        * ``key`` = `["A", "E"]`
        * ``axis`` = `1`


        If the data received on port ``i`` is: ::

                                              A         B        ...         E         F
            2017-12-31 23:59:59.998745401  0.185133  0.541901    ...     0.806561  0.658783
            2018-01-01 00:00:00.104507143  0.692277  0.849196    ...     0.221209  0.987668
            2018-01-01 00:00:00.202319939  0.944059  0.039427    ...     0.180575  0.567945

        The data provided on port ``o`` will be: ::

                                               A         E
            2017-12-31 23:59:59.998745401  0.185133  0.806561
            2018-01-01 00:00:00.104507143  0.692277  0.221209
            2018-01-01 00:00:00.202319939  0.944059  0.180575

    References:

        See the documentation of `pandas.DataFrame.loc <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.loc.html>`_  .

    """

    def __init__(self, key, axis=1) :


        """
        Args:
           key (str|tuple): Label selection specification.
           axis (int): Axis to query the label from (`0` or `1`). Default: `1`.

        """

        self._axis = axis
        if type(key) not in [list, tuple]:
            self._key = [key]
        else:
            self._key = key
        self._check_args = False

    def update(self):

        self.o = self.i
        if self.i.data is not None:
            if not self.i.data.empty:

                if self._axis == 0:
                    self.o.data = self.i.data.loc[self._key, :]
                elif self._axis == 1:
                    if not self._check_key:
                        try:
                            self.o.data = self.i.data.loc[:, self._key]
                            self._check_args = True
                        except KeyError as e:
                            logging.error(e)
                            raise ValueError (e)
                    else:
                        self.o.data = self.i.data.loc[:, self._key]
