"""timeflux.nodes.random: generate random data"""

import random
import numpy as np
from timeflux.core.node import Node


class Random(Node):
    def __init__(
        self,
        columns=5,
        rows_min=2,
        rows_max=10,
        value_min=0,
        value_max=9,
        names=None,
        seed=None,
    ):
        """Return random integers from value_min to value_max (inclusive)"""
        self._rows_min = rows_min
        self._rows_max = rows_max
        self._value_min = value_min
        self._value_max = value_max
        self._names = names
        self._columns = len(names) if names else columns
        random.seed(seed)
        np.random.seed(seed)

    def update(self):
        rows = random.randint(self._rows_min, self._rows_max)
        shape = (rows, self._columns)
        self.o.set(
            np.random.randint(self._value_min, self._value_max + 1, size=shape),
            names=self._names,
        )
