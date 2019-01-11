from timeflux.core.node import Node
from importlib import import_module
import pandas as pd
from timeflux.core.exceptions import TimefluxException
from timeflux.core.io import Port


class Expression(Node):
    """ Evaluate a Python expression as a string.

    This nodes uses `eval` method from pandas to evaluate a Python expression as a string on the data.
    The expression can be evaluated either on the input ports (``eval_on`` = `ports`) or on the input columns (``eval_on`` = `columns`).

    The following arithmetic operations are supported: +, -, *, /, **, %, //
    (python engine only) along with the following boolean operations: | (or), & (and), and ~ (not).


    Attributes:
        i (Port): default data input, expects DataFrame.
        i_* (Port, optional): data inputs when ``eval_on`` is `ports`.
        o (Port): default output, provides DataFrame.


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
        Hence, the variables on which the expression is applied are the columns of the data from default input port.
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

    def __init__(self, expr="port_i", eval_on="ports", kwargs={}):
        """
            Args:
               expr (str) : Expression of the function to apply to each column or row.
               eval_on (`columns` | `ports`): Variable on which the expression is evaluated. Default: `ports`
                      If `columns`, the variables passed to the expression are the columns of the data in default input port.
                      If `ports`, the variables passed to the expression are the data of the input ports.
               kwargs : Additional keyword arguments to pass as keywords arguments to `pandas.eval`:
                            `{"parser": "pandas", "engine": None, "resolvers": None,"level": None, "target": None }`

        """
        if ("global_dict" in kwargs.keys()):
            raise (ValueError(
                "global_dict cannot be passed as additionnal arguments for pandas.eval "
            ))
        if ('local_dict' not in kwargs.keys()) & (eval_on == "ports"):
            self._static_local_dict = {}
            self._static_kwargs = kwargs
        else:
            if "local_dict" in kwargs.keys(): self._static_local_dict = kwargs["local_dict"]

            self._static_kwargs = {
                k: kwargs[k]
                for k in set(list(kwargs.keys())) - set(["local_dict"])
                }

        if eval_on == "ports":
            self._eval_on = eval_on
            ## check the configuration
            foo_ports = {"i_" + str(k): Port() for k in range(5)}
            foo_ports["i"] = Port()
            for name, port in foo_ports.items():
                port.data = pd.DataFrame([[1, 2], [3, 4]])

            if self._ports_ready(foo_ports):
                kwargs = self._update_locals(foo_ports)
            try:
                _ = pd.eval(expr=expr, **kwargs)
                self._expr = expr
            except ValueError:
                raise (ValueError("expr is not valid"))
        elif eval_on == "columns":
            self._eval_on = eval_on
            self._kwargs = kwargs
            self._expr = expr
        else:
            raise (
                ValueError,
                "eval_on should be 'ports' or 'columns', got {eval_on}".format(
                    eval_on=eval_on))

    def _ports_ready(self, ports):
        return any([
            port.data is not None for name, port in ports.items()
            if name == 'i' or name.startswith('i_') ])

    def _update_locals(self, ports):
        _static_local_dict = {**self._static_local_dict, **{name: port.data for name, port in ports.items() if
                                                            name == 'i' or name.startswith('i_')}}
        return {"local_dict": _static_local_dict, **self._static_kwargs}

    def update(self):
        if self._eval_on == "ports":
            if self.ports is not None:
                if self._ports_ready(self.ports):
                    kwargs = self._update_locals(self.ports)
                    self.o.data = pd.eval(expr=self._expr, **kwargs)
                    meta = {
                        k: v
                        for meta in [
                        port.meta for name, port in self.ports.items()
                        if name == 'i' or name.startswith('i_')
                        if port.meta is not None
                    ] for k, v in meta.items()
                    }
                    if meta:
                        self.o.meta = {
                            k: v
                            for meta in [
                            port.meta for name, port in self.ports.items()
                            if name == 'i' or name.startswith('i_')
                        ] for k, v in meta.items() if meta is not None
                        }
        elif self._eval_on == "columns":
            self.o = self.i
            if self.i.data is not None:
                self.o.data = self.i.data.eval(expr=self._expr, **self._kwargs)
