from timeflux.core.node import Node
from importlib import import_module
import pandas as pd

class ApplyMethod(Node):
    """ Apply a function along an axis of the DataFrame.

    This node applies a function along an axis of the DataFrame.
    Objects passed to the function are Series objects whose index is
    either the DataFrame's index (``axis`` = `0`) or the DataFrame's columns
    (``axis`` = `1`).

    Attributes:
        i (Port): default data input, expects DataFrame.
        o (Port): default output, provides DataFrame.

    Notes:

        Note that the passed function will receive ndarray objects for performance purposes.
        For universal functions, ie. transformation from n_array to n_array, input and output ports have the same size.
        For reducing function, ie. from n_array to scalar, output ports's index is set to first (if ``closed`` = `left`), last (if ``closed`` = `right`), or middle (if ``closed`` = `center`)


    .. todo::
        Allow expanding functions such as n_array to nk_array (with XArray usage)

    Example:

        Universal function: in this example, we apply `numpy.sqrt` to each value of the data. Shapes of input and output data are the same.

        * ``module_name`` = `numpy`
        * ``method_name`` = `sqrt`
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

        * ``module_name`` = `numpy`
        * ``method_name`` = `sum`
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

    def __init__(self, func, module_name="numpy", method_name="mean", method_type="universal", kwds={}, axis=0 , closed="right"):
        """
           Args:
               func (func): custom function specified directely that takes as input a n_array (eg. lambda x: x+1).
               module_name (str): name of the module to import, in which the method is defined. Default: `numpy`
               method_name (str): name of the callable method to apply on the data. Default: `mean`
               method_type (str|`universal`): {`universal`, `reduce`, `expand` }. Default: `universal`.
                             -  `universal` if function is a transformation from n_array to n_array
                             -  `reduce` if function is a transformation from n_array to scalar
                             -  `expand` if function is a transformation from n_array to nk_array [not yet implemented]
               kwds (dict): dictionnary with additional keyword arguments to pass as keywords arguments to `func`.
               axis (int) : if 0, the transformation is applied to columns, if 1 to rows. Default: `0`
               closed (int) : {`left`, `right`, `center`}: timestamp to transfer in the output, only when method_type is "reduce" and axis = 0, in which case, the output port's lenght is 1. Default: `right`

        """

        self._axis = axis
        if func is not None:
            self._method_to_call = func
            self._kwds = {}
        else:
            try:
                m = import_module(module_name)
            except ImportError:
                raise ValueError ("Could not import module {module_name}".format(module_name=module_name))
            try:
                self._method_to_call = getattr(m, method_name)
            except AttributeError:
                raise ValueError  ("Module {module_name} has no attribute {method_name}".format(module_name=module_name, method_name=method_name))

        if method_type == "reduce":
            self._apply_kwds = {"raw":True, "result_type": "reduce", "axis": axis }
            if axis == 0:
                self._closed = closed
                self._reduce= "row"
            elif axis == 1:
                self._reduce = "col"
            else:
                raise ValueError ("Apply axis should be 0 or 1 given: {axis}".format(mode=axis))
        else:
            self._reduce = None
            self._apply_kwds = {"raw": True, "axis": axis}

        if not callable(self._method_to_call):
            raise ValueError  ("Could not call the method {methode_name}".format(method_name=method_name))
        else:
            try:
                foo_i = pd.DataFrame(data=[[0,1], [2,3]])
                foo_i.apply(**self._apply_kwds, **kwds, func=self._method_to_call)
                self._kwds = kwds
            except TypeError:
                raise TypeError ("kwds don't match the method {method_name} definition".format(method_name=method_name))

    def update(self):
        self.o.meta = self.i.meta
        if self.i.data is not None:
            self.o.data = self.i.data.apply(**self._apply_kwds, **self._kwds, func=self._method_to_call)
            if  self._reduce:
                if self._reduce == "row":
                    if self._closed == "right":
                        index_to_keep= -1
                    if self._closed == "left":
                        index_to_keep = 0
                    if self._closed == "middle":
                        index_to_keep = len(self.i.data)//2
                    self.o.data = self.i.data.apply(**self._apply_kwds, **self._kwds, func=self._method_to_call).to_frame(self.i.data.index[index_to_keep]).T
                elif self._reduce == "col":
                    self.o.data = self.i.data.apply(**self._apply_kwds, **self._kwds,
                                                    func=self._method_to_call).to_frame()