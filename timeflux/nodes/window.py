"""Basic sliding window."""

from timeflux.core.node import Node
import pandas as pd


class Window(Node):

    def __init__(self, length, step=None):
        """
        Initialize the buffer.

        Args:
            length (float): The length of the window, in seconds.
            step (float): The minimal sliding step, in seconds.
                If None (the default), the step will be set to the window duration.
                If 0, the data will be sent as soon as it is available.
        """

        if step is None: step = length
        self._length = pd.Timedelta(seconds=length)
        self._step = pd.Timedelta(seconds=step)
        self._buffer = None
        self._overlap = self._step < self._length  # whether epoch should be overlapping

    def update(self):
        # Return immediately if we don't have any data
        if not self.i.ready():
            return
        # Append new data
        if self._buffer is None:
            self._buffer = self.i.data
            self._updated = self.i.data.index[0]
        else:
            self._buffer = self._buffer.append(self.i.data)
        # Time range
        high = self._buffer.index[-1]
        low = high - self._length
        # Make sure we have enough data
        if self._buffer.index[0] <= low:
            # Step
            if high - self._updated >= self._step:
                actual_step = (high - self._updated)
                if self._overlap and (actual_step > self._step):
                    self.logger.warning(f'Received to much data to provide requested overlapping windows. '
                                        f'Step is of {actual_step} instead of {self._step}.')
                # Clear old data
                self._buffer = self._buffer[self._buffer.index > low]  # self._buffer[low:]
                # Remember the last time the buffer was updated
                self._updated = high
                # Output
                self.o.data = self._buffer
                self.o.meta = self.i.meta
