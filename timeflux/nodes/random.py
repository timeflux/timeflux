"""timeflux.nodes.random: generate random data"""

import random
import time
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
        """Return random integers between value_min and value_max (inclusive)"""
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


class Signal(Node):
    def __init__(self, channels=5, rate=100, amplitude=200, names=None, seed=None):
        """Return random floats within the given peak-to-peak amplitude"""
        self._rate = rate
        self._amplitude = amplitude
        self._names = names
        self._channels = len(names) if names else channels
        self._updated = time.time()
        random.seed(seed)
        np.random.seed(seed)

    def update(self):
        now = time.time()
        elapsed = now - self._updated
        self._updated = now
        samples = round(elapsed / (1 / self._rate))
        if samples > 0:
            shape = (samples, self._channels)
            self.o.set(
                np.random.random_sample(shape) * self._amplitude
                - (self._amplitude / 2),
                names=self._names,
                meta={"rate": self._rate},
            )
