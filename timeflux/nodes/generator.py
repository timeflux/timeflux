"""timeflux.nodes.generator: Generate signals """
import random

import numpy as np

from timeflux.core.node import Node
from timeflux.core.registry import Registry
from timeflux.helpers.clock import time_to_float, now


class Gaussian(Node):
    def __init__(self, columns, rows_min, rows_max, mu=0, std=1):
        """Returns random samples from a normal (Gaussian) distribution """
        self._columns = columns
        self._rows_min = rows_min
        self._rows_max = rows_max
        self._mu = mu  # Mean (“centre”) of the distribution.
        self._std = std  # Std ("scale") of the distribution

    def update(self):
        rows = random.randint(self._rows_min, self._rows_max)
        shape = (rows, self._columns)
        self.o.set(np.random.normal(loc=self._mu, scale=self._std, size=shape))


class Sinus(Node):
    """Return a sinusoidal signal sampled to registry rate.

    This node generates a sinusoidal signal of chosen frequency and amplitude.
    Note that at each update, the node generate one row, so its sampling rate
    equals the graph parsing rate (given by the Registry).

    Attributes:
        o (Port): Default output, provides DataFrame.

    Example:s
        .. literalinclude:: /../../timeflux_example/test/graphs/sinus.yaml
           :language: yaml
    """

    def __init__(self, amplitude=1, rate=1, name='sinus'):
        self._amplitude = amplitude
        self._rate = rate
        self._name = name
        self._start = None

    def update(self):
        timestamp = now()
        float = time_to_float(timestamp)
        if self._start is None:
            self._start = float

        values = [self._amplitude * np.sin(2 * np.pi * self._rate * (float - self._start))]
        self.o.set(values, names=[self._name])
        self.o.meta = {'rate': Registry.rate}
