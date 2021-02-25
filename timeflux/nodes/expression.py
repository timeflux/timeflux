import numpy as np
import pandas as pd
from timeflux.core.io import Port
from timeflux.core.node import Node


class Expression(Node):
    """Evaluate a Python expression as a string.

    This nodes uses `eval` method from pandas to evaluate a Python expression
    as a string on the data.
    The expression can be evaluated either on the input ports
    (``eval_on`` = `ports`) or on the input columns (``eval_on`` = `columns`).

    The following arithmetic operations are supported: +, -, *, /, **, %, //
    (python engine only) along with the following boolean operations:
    | (or), & (and), and ~ (not).


    Attributes:
        i (Port): default data input, expects DataFrame.
        i_* (Port, optional): data inputs when ``eval_on`` is `ports`.
        o (Port): default output, provides DataFrame.

    Args:
       expr (str) : Expression of the function to apply to each column or row.
       eval_on (`columns` | `ports`): Variable on which the expression is evaluated. Default: `ports`
              If `columns`, the variables passed to the expression are the columns of the data in default input port.
              If `ports`, the variables passed to the expression are the data of the input ports.
       kwargs : Additional keyword arguments to pass as keywords arguments to `pandas.eval`:
                    `{'parser': 'pandas', 'engine': None, 'resolvers': None,'level': None, 'target': None }`

    Example:

        In this example, we eval arithmetic expression on the input ports : `o = i_1 + i_2`.
        Hence, the variables on which the expression is applied are the data of the ports.

        * ``expr`` = `i_1 + i_2`
        * ``eval_on`` = `ports`

        The nodes expects two data inputs :

        On port ``i_1``:  ::

                                  0  1
            2018-01-01 00:00:00   5  8
            2018-01-01 00:00:01   9  5
            2018-01-01 00:00:02  10  4
            2018-01-01 00:00:03   5  5

        On port ``i_2``:  ::

                                  0  1
            2018-01-01 00:00:00   1  3
            2018-01-01 00:00:01   1  5
            2018-01-01 00:00:02  10  1
            2018-01-01 00:00:03   2  1

        It returns one data output that is ``i_1.data`` + ``i_2.data`` :

        On port ``o``:  ::

                                  0   1
            2018-01-01 00:00:00   6  11
            2018-01-01 00:00:01  10  10
            2018-01-01 00:00:02  20   5
            2018-01-01 00:00:03   7   6

    Example:

        In this example, we eval an arithmetic expression on columns  : `col3 = col2 + col1`
        Hence, the variables on which the expression is applied are the columns
        of the data from default input port.
        We set:

        * ``expr`` = `col2 = col1 + col0`
        * ``eval_on`` = `columns`

        The node expects data with columns `col0` and `col1` on the default port ``i``: ::


                                 col0  col1
            2018-01-01 00:00:00     5     8
            2018-01-01 00:00:01     9     5

        It returns data with an appended column `col2` on port ``o``: ::

                                 col0  col1  col2
            2018-01-01 00:00:00     5     8    13
            2018-01-01 00:00:01     9     5    14

    References:

        See the documentation of `pandas.eval <https://pandas.pydata.org/pandas-docs/version/0.21/generated/pandas.eval.html>`_ .

    """

    def __init__(self, expr, eval_on, **kwargs):

        if "global_dict" in kwargs:
            raise (
                ValueError(
                    "global_dict cannot be passed as additional arguments for pandas.eval "
                )
            )

        self._eval_on = eval_on
        self._kwargs = kwargs
        self._expr = expr
        self._expr_ports = None

    def update(self):
        self.o.meta = self.i.meta
        if self._eval_on == "ports":
            if self._expr_ports is None:
                self._expr_ports = [
                    port_name
                    for port_name, _, _ in self.iterate("i_*")
                    if port_name in self._expr
                ]
            _local_dict = {
                port_name: self.ports.get(port_name).data
                for port_name in self._expr_ports
            }
            if np.any([data is None or data.empty for data in _local_dict.values()]):
                return
            self.o.data = pd.eval(
                expr=self._expr, local_dict=_local_dict, **self._kwargs
            )
            for port_name in self._expr_ports:
                self.o.meta.update(self.ports.get(port_name).meta)
        elif self._eval_on == "columns":
            self.o = self.i
            if self.i.data is not None and not self.i.data.empty:
                self.o.data = self.i.data.eval(expr=self._expr, **self._kwargs)
