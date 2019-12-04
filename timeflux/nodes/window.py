"""Basic sliding window."""

from timeflux.core.node import Node
import pandas as pd


class Window(Node):

    def __init__(self, length, step=None):
        """
        Initialize the buffer.

        Args:
            length (float): The length of the window, in seconds.
            step (float): The sliding step, in seconds.
                If None (the default), the step will be set to the window duration.
                If 0, the data will be sent as soon as it is available.
        """

        if step is None: step = length
        self._length = pd.Timedelta(seconds=length)
        self._step = pd.Timedelta(seconds=step)
        self._buffer = None
        self._updated = pd.Timestamp(0)

    def update(self):
        # Return immediately if we don't have any data
        if not self.i.ready():
            return
        # Append new data
        if self._buffer is None:
            self._buffer = self.i.data
        else:
            self._buffer = self._buffer.append(self.i.data)
        # Time range
        high = self._buffer.index[-1]
        low = high - self._length
        # Make sure we have enough data
        if self._buffer.index[0] <= low:
            # Step
            if high - self._updated >= self._step:
                # Clear old data
                self._buffer = self._buffer[low:]
                # Remember the last time the buffer was updated
                self._updated = high
                # Output
                self.o.data = self._buffer
                self.o.meta = self.i.meta
