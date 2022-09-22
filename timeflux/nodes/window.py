"""Sliding windows"""

import pandas as pd
from timeflux.core.node import Node
from timeflux.core.exceptions import WorkerInterrupt


class Slide(Node):

    """Sliding windows.

    This node generates sliding windows from the default input stream.
    The durations are given in seconds, but these are converted in samples. Therefore, the outputs will always have the same number of datapoints, even if there is jitter in the input stream.
    Multiple, overlapping windows are authorized. Each concurrent window is assigned to a numbered `Port`. For convenience, the first window is bound to the default output, so you can avoid enumerating all output ports if you expects only one window.

    Attributes:
        i (Port): Default data input, expects DataFrame.
        o (Port): Default output, provides DataFrame and meta.
        o_* (Port): Dynamic outputs, provide DataFrame and meta.

    Args:
        length (float): The length of the window, in seconds.
        step (float|None): The minimal sliding step, in seconds.
            If None (the default), the step will be set to the length of the window.
        rate (float): The rate of the input stream.
            If None (the default), it will be taken from the meta data.

    """

    def __init__(self, length=0.6, step=None, rate=None):

        if step == None or step <= 0:
            step = length
        self._rate = rate
        self._length_seconds = length
        self._step_seconds = step
        self._length_samples = 0
        self._step_samples = 0
        self._index = None
        self._windows = []

    def update(self):

        if not self.i.ready():
            return

        # We need a rate, either as an argument or from the input meta
        if not self._rate:
            if not "rate" in self.i.meta:
                self.logger.error("Rate is not specified")
                raise WorkerInterrupt()
            self._rate = self.i.meta["rate"]

        # Convert durations to samples
        if self._index == None:
            self._length_samples = round(self._length_seconds * self._rate)
            self._step_samples = round(self._step_seconds * self._rate)
            if self._length_samples == 0:
                self._length_samples = 1
            if self._step_samples == 0:
                self._step_samples = 1
            self._index = 0

        # Append to existing windows
        for index, window in enumerate(self._windows):
            stop = self._length_samples - len(window)
            self._windows[index] = pd.concat([window, self.i.data[0:stop]])

        # Create new windows
        for index in range(self._index, len(self.i.data), self._step_samples):
            if index >= 0:
                start = index
                stop = index + self._length_samples
                self._windows.append(self.i.data[start:stop])
        self._index = index - len(self.i.data)

        # Send complete windows
        complete = 0
        for index, window in enumerate(self._windows):
            if len(window) == self._length_samples:
                o = getattr(self, "o_" + str(index))
                o.data = window
                complete += 1
        if complete > 0:
            del self._windows[:complete]  # Unqueue
            self.o = self.o_0  # Bind default output to the first epoch


class Window(Node):

    """Provide sliding windows.

    Attention:
        This node is deprecated and will be removed in future versions. Use `Slide` instead.

    Attributes:
        i (Port): Default data input, expects DataFrame.

    Args:
        length (float|int): The length of the window, in seconds or samples.
        step (float|int|None): The minimal sliding step, in seconds or samples.
            If None (the default), the step will be set to the length of the window.
            If 0, the data will be sent as soon as it is available.
        index (string):
            If "time" (the default), the length of the window is in seconds.
            If "sample", the length of the window is in samples.
        epochs (boolean):
            Whether the defaut output should be bound to an numbered output, thus
            simulating an epoch. This could be useful if piped to a Machine Learning
            node.

    """

    def __init__(self, length, step=None, index="time", epochs=False):

        if index not in ["time", "sample"]:
            raise ValueError("Invalid `index` value.")
        mixin = TimeWindow if index == "time" else SampleWindow
        self.__class__ = type("SlidingWindow", (mixin, Window), {})
        self.__init__(length, step)
        if epochs:
            self.bind("o", "o_0")

    def update(self):

        pass


class TimeWindow(Node):
    def __init__(self, length, step=None):

        if step is None:
            step = length
        if step > length:
            raise ValueError("`step` must be less than or equal to `length`.")
        self._silence = True if step == 0 else False
        self._length = pd.Timedelta(seconds=length)
        self._step = pd.Timedelta(seconds=step)
        self._buffer = None

    def update(self):

        # Return immediately if we don't have any data
        if not self.i.ready():
            return

        # Sanity check
        if not self.i.data.index.is_monotonic:
            self.logger.warning("Indices are non-monotonic.")

        # Append new data
        if self._buffer is None:
            self._buffer = self.i.data
        else:
            self._buffer = pd.concat([self._buffer, self.i.data])

        # Update the default output if we have enough data
        low = self._buffer.index[0]
        high = low + self._length
        if self._buffer.index[-1] >= high:
            self.o.data = self._buffer[self._buffer.index < high]
            self.o.meta = self.i.meta
            self._buffer = self._buffer[self._buffer.index >= low + self._step]

        # Make sure we are not overflowing
        if (
            not self._buffer.empty
            and (self._buffer.index[-1] - self._buffer.index[0]) > self._length
        ):
            if not self._silence:
                self.logger.warning(
                    "This node is falling behind: it is receiving "
                    "more data than it can send. Check the window "
                    "parameters and the graph rate."
                )
            self._buffer = self._buffer[
                self._buffer.index > self._buffer.index[-1] - self._length + self._step
            ]


class SampleWindow(Node):
    def __init__(self, length, step=None):

        if step is None:
            step = length
        self._length = length
        self._step = step
        self._buffer = None

    def update(self):

        if not self.i.ready():
            return

        # Append new data
        if self._buffer is None:
            self._buffer = self.i.data
        else:
            self._buffer = pd.concat([self._buffer, self.i.data])

        # Make sure we have enough data
        if len(self._buffer) >= self._length:
            self.o.data = self._buffer[-self._length :]
            self.o.meta = self.i.meta
            # Step
            self._buffer = self.o.data[self._step :]
