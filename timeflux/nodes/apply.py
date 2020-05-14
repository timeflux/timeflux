"""Arbitrary operations on DataFrames"""

from timeflux.core.node import Node
from importlib import import_module
import pandas as pd


class ApplyMethod(Node):
    """Apply a function along an axis of the DataFrame.

    This node applies a function along an axis of the DataFrame.
    Objects passed to the function are Series objects whose index is
    either the DataFrame's index (``axis`` = `0`) or the DataFrame's columns
    (``axis`` = `1`).

    Attributes:
        i (Port): default data input, expects DataFrame.
        o (Port): default output, provides DataFrame.

    Args:
       func (func): custom function specified directely that takes as input a n_array (eg. lambda x: x+1). Default: None.
       method (str): name of the module to import, in which the method is defined. eg. `numpy.mean`.
       apply_mode (str`): {`universal`, `reduce`, `expand` }. Default: `universal`.
             -  `universal` if function is a transformation from n_array to n_array
             -  `reduce` if function is a transformation from n_array to scalar
             -  `expand` if function is a transformation from n_array to nk_array [not yet implemented]
       axis (int) : if 0, the transformation is applied to columns, if 1 to rows. Default: `0`.
       closed (str) : {`left`, `right`, `center`}: timestamp to transfer in the output, only when method_type is "reduce" and axis = 0, in which case, the output port's lenght is 1. Default: `right`.
       kwargs:  additional keyword arguments to pass as keywords arguments to `func`.

    Notes:

        Note that the passed function will receive ndarray objects for performance purposes.
        For universal functions, ie. transformation from n_array to n_array, input and output ports have the same size.
        For reducing function, ie. from n_array to scalar, output ports's index is set to first (if ``closed`` = `left`), last (if ``closed`` = `right`), or middle (if ``closed`` = `center`)


    .. todo::
        Allow expanding functions such as n_array to nk_array (with XArray usage)

    Example:

        Universal function: in this example, we apply `numpy.sqrt` to each value of the data. Shapes of input and output data are the same.

        * ``method`` = `numpy.sqrt`
        * ``method_type`` = `universal`

        If data in input port is ``i`` is: ::

                                               0
            2018-10-25 07:33:41.871131         9.0
            2018-10-25 07:33:41.873084         16.0
            2018-10-25 07:33:41.875037         1.0
            2018-10-25 07:33:41.876990         4.0



        It returns the squared root of the data on port ``o``: ::

                                               0
            2018-10-25 07:33:41.871131         3.0
            2018-10-25 07:33:41.873084         4.0
            2018-10-25 07:33:41.875037         1.0
            2018-10-25 07:33:41.876990         2.0

    Example:

        Reducing function: in this example, we apply `numpy.sum` to each value of the data. Shapes of input and output data are not the same.
        We set:

        * ``method`` = `numpy.sum`
        * ``method_type`` = `reduce`
        * ``axis`` = `0`
        * ``closed`` = `right`

        If data in input port is ``i`` is: ::

                                               0        1
            2018-10-25 07:33:41.871131         9.0      10.0
            2018-10-25 07:33:41.873084         16.0     2.0
            2018-10-25 07:33:41.875037         1.0      5.0
            2018-10-25 07:33:41.876990         4.0      2.0



        It returns the sum amongst row axis on port ``o``: ::

                                               0        1
            2018-10-25 07:33:41.876990         30.0     19.0

    References:

        See the documentation of `pandas.apply <https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.apply.html>`_ .


    """

    def __init__(
        self,
        method,
        apply_mode="universal",
        axis=0,
        closed="right",
        func=None,
        **kwargs,
    ):

        self._axis = axis
        self._closed = closed
        self._kwargs = kwargs
        self._apply_mode = apply_mode

        if func is not None:
            self._func = func
        else:
            module_name, function_name = method.rsplit(".", 1)

            try:
                module = import_module(module_name)
            except ImportError:
                raise ImportError(f"Could not import module {module_name}")
            try:
                self._func = getattr(module, function_name)
            except AttributeError:
                raise ValueError(
                    f"Module {module_name} has no function {function_name}"
                )

            if not callable(self._func):
                raise ValueError(f"Could not call the method {self._methode_name}")

            self._kwargs.update({"raw": True, "axis": axis})
            if self._apply_mode == "reduce":
                self._kwargs["result_type"] = "reduce"

    def update(self):

        if not self.i.ready():
            return

        self.o.meta = self.i.meta
        self.o.data = self.i.data.apply(func=self._func, **self._kwargs)
        if self._apply_mode == "reduce":
            if self._axis == 0:
                if self._closed == "right":
                    index_to_keep = self.i.data.index[-1]
                elif self._closed == "left":
                    index_to_keep = self.i.data.index[0]
                else:  # self._closed == 'middle':
                    index_to_keep = self.i.data.index[len(self.i.data) // 2]
                self.o.data = pd.DataFrame(self.o.data, columns=[index_to_keep]).T
            else:  # self._axis == 1:
                self.o.data = self.o.data.to_frame()
