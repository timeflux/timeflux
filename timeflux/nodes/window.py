"""Basic sliding window."""

from timeflux.core.node import Node
import pandas as pd


class Window(Node):

    def __init__(self, length, step=None):
        """
        Initialize the buffer.

        Args:
            length (float): The length of the window, in seconds.
            step (float|None): The minimal sliding step, in seconds.
                If None (the default), the step will be set to the window duration.
                If 0, the data will be sent as soon as it is available.
        """
        if step is None:
            step = length
        if step > length:
            raise ValueError('`step` must be less than or equal to `length`.')
        self._length = pd.Timedelta(seconds=length)
        self._step = pd.Timedelta(seconds=step)
        self._buffer = None

    def update(self):
        # Return immediately if we don't have any data
        if not self.i.ready():
            return
        # Sanity check
        if not self.i.data.index.is_monotonic:
            self.logger.warning('Indices are non-monotonic.')
        # Append new data
        if self._buffer is None:
            self._buffer = self.i.data
        else:
            self._buffer = self._buffer.append(self.i.data)
        # Update the default output if we have enough data
        low = self._buffer.index[0]
        high = low + self._length
        if self._buffer.index[-1] >= high:
            self.o.data = self._buffer[self._buffer.index < high]
            self.o.meta = self.i.meta
            self._buffer = self._buffer[self._buffer.index >= low + self._step]
        # Make sure we are not overflowing
        if not self._buffer.empty and (self._buffer.index[-1] - self._buffer.index[0]) > self._length:
            self.logger.warning('This node is falling behind: it is receiving '
                                'more data than it can send. Check the window '
                                'parameters and the graph rate.')
            self._buffer = self._buffer[self._buffer.index > self._buffer.index[-1] - self._length + self._step]
