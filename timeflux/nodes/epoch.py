"""Epoching nodes"""

import numpy as np
import pandas as pd
import json
import xarray as xr
from timeflux.core.node import Node
from timeflux.core.exceptions import WorkerInterrupt
from timeflux.helpers.port import match_events


class Epoch(Node):
    """Event-triggered epoching.

    This node continuously buffers a small amount of data (of a duration of ``before`` seconds) from the default input stream.
    When it detects a marker matching the ``event_trigger`` in the ``label`` column of the event input stream, it starts accumulating data for ``after`` seconds.
    It then sends the epoched data to an output stream, and sets the metadata to a dictionary containing the triggering marker and optional event data.
    Multiple, overlapping epochs are authorized. Each concurrent epoch is assigned its own `Port`. For convenience, the first epoch is bound to the default output, so you can avoid enumerating all output ports if you expects only one epoch.

    Attributes:
        i (Port): Default data input, expects DataFrame.
        i_events (Port): Event input, expects DataFrame.
        o (Port): Default output, provides DataFrame and meta.
        o_* (Port): Dynamic outputs, provide DataFrame and meta.

    Args:
        event_trigger (string): The marker name.
        before (float): Length before onset, in seconds.
        after (float): Length after onset, in seconds.

    Example:
        .. literalinclude:: /../examples/epoch.yaml
           :language: yaml

    """

    def __init__(self, event_trigger, before=0.2, after=0.6):

        self._event_trigger = event_trigger
        self._before = pd.Timedelta(seconds=before)
        self._after = pd.Timedelta(seconds=after)
        self._buffer = None
        self._epochs = []

    def update(self):

        # Append to main buffer
        if self.i.data is not None:
            if not self.i.data.empty:
                if self._buffer is None:
                    self._buffer = self.i.data
                else:
                    self._buffer = self._buffer.append(self.i.data)

        # Detect onset
        matches = match_events(self.i_events, self._event_trigger)
        if matches is not None:
            for index, row in matches.iterrows():
                # Start a new epoch
                low = index - self._before
                high = index + self._after
                if self._buffer is not None:
                    if not self._buffer.index.is_monotonic:
                        self.logger.warning("Index must be monotonic. Skipping epoch.")
                        return
                    try:
                        context = json.loads(row["data"])
                    except json.JSONDecodeError:
                        context = row["data"]
                    self._epochs.append(
                        {
                            "data": self._buffer[low:high],
                            "meta": {
                                "onset": index,
                                "context": context,
                                "before": self._before.total_seconds(),
                                "after": self._after.total_seconds(),
                            },
                        }
                    )

        # Trim main buffer
        if self._buffer is not None:
            low = self._buffer.index[-1] - self._before
            self._buffer = self._buffer[low:]

        # Update epochs
        if self._epochs and self.i.ready():
            complete = 0
            for epoch in self._epochs:
                high = epoch["meta"]["onset"] + self._after
                last = self.i.data.index[-1]
                if epoch["data"].empty:
                    low = epoch["meta"]["onset"] - self._before
                    mask = (self.i.data.index >= low) & (self.i.data.index <= high)
                else:
                    low = epoch["data"].index[-1]
                    mask = (self.i.data.index > low) & (self.i.data.index <= high)
                # Append
                epoch["data"] = epoch["data"].append(self.i.data[mask])
                # Send if we have enough data
                if last >= high:
                    o = getattr(self, "o_" + str(complete))
                    o.data = epoch["data"]
                    o.meta = {"epoch": epoch["meta"]}
                    complete += 1
            if complete > 0:
                del self._epochs[:complete]  # Unqueue
                self.o = self.o_0  # Bind default output to the first epoch


class Trim(Node):
    """Trim data so epochs are of equal length.

    Because real-time data is often jittered, the `Epoch` node is not always able to
    provide dataframes of equal dimensions. This can be problematic if the data is
    further processed by the `Pipeline` node, for example. This simple node takes care
    of trimming the extra samples. It should be placed just after an `Epoch` node.

    Attributes:
        i_* (Port): Epoched data input, expects DataFrame.
        o_* (Port): Trimmed epochs, provides DataFrame and meta.

    Args:
        samples (int): The maximum number of samples per epoch.
                       If `0`, the size of the first epoch is used.

    """

    def __init__(self, samples=0):
        self.samples = samples

    def update(self):
        ports = []
        for _, _, port in self.iterate("i_*"):
            if port.ready():
                if self.samples == 0:
                    self.samples = len(port.data)
                if len(port.data) < self.samples:
                    self.logger.warn(
                        f"Epoch rejected: not enough sample ({len(port.data)}<{self.samples})"
                    )
                else:
                    port.data = port.data.head(self.samples)
                    ports.append(port)
        for i, port in enumerate(ports):
            o = getattr(self, f"o_{i}")
            o.data = port.data
            o.meta = port.meta


class ToXArray(Node):
    """Convert multiple epochs to DataArray

    This node iterates over input ports with valid epochs, concatenates them on the
    first axis, and creates a XArray with dimensions ('epoch', 'time', 'space') where
    epoch corresponds to th input ports, time to the ports data index and space to the
    ports data columns.
    A port is considered to be valid if it has meta with key 'epoch' and data with
    expected number of samples.
    If some epoch have an invalid length (which happens when the data has jitter), the
    node either raises a warning, an error or pass.

    Attributes:
        i_* (Port): Dynamic inputs, expects DataFrame and meta.
        o (Port): Default output, provides DataArray and meta.

    Args:
        reporting (string| None): How this function handles epochs with
            invalid length: `warn` will issue a warning with :py:func:`warnings.warn`,
            `error` will raise an exception, `None` will ignore it.
        output (`DataArray`|`Dataset`): Type of output to return
        context_key (string|None): If output type is `Dataset`, key to define the
            target of the event. If `None`, the whole context is considered.

    """

    def __init__(self, reporting="warn", output="DataArray", context_key=None):

        self._reporting = reporting
        self._output = output
        self._context_key = context_key
        self._columns = self._before = self._after = None
        self._ready = False

    def update(self):

        if not self._ready:
            ports_ready = [port for _, _, port in self.iterate("i*") if port.ready()]
            if len(ports_ready) < 1:
                return
            # initialize attributes on first ready port
            port = ports_ready[0]
            if port.ready():
                self._columns = port.data.columns
                self._before = port.meta["epoch"]["before"]
                self._after = port.meta["epoch"]["after"]
                self._num_times = len(port.data)
                self._times = pd.TimedeltaIndex(
                    data=np.linspace(-self._before, self._after, self._num_times),
                    unit="s",
                )
                self._rate = 1 / (self._times[1] - self._times[0]).total_seconds()
                self._ready = True

        ports_ready = [
            port for _, _, port in self.iterate(name="i*") if self._valid_port(port)
        ]

        if not ports_ready:
            return

        list_onset = [port.meta["epoch"].get("onset") for port in ports_ready]
        list_context = [port.meta["epoch"].get("context") for port in ports_ready]
        list_epochs = [port.data for port in ports_ready]

        data = np.stack([epoch.values for epoch in list_epochs], axis=0)

        meta = {
            "epochs_context": list_context,
            "epochs_onset": list_onset,
            "rate": self._rate,
        }

        if self._output == "DataArray":
            if self._context_key is not None:
                data_array = xr.DataArray(
                    data,
                    dims=("target", "time", "space"),
                    coords=(
                        [self._extract_target(context) for context in list_context],
                        self._times,
                        self._columns,
                    ),
                )
            else:
                data_array = xr.DataArray(
                    data,
                    dims=("epoch", "time", "space"),
                    coords=(np.arange(data.shape[0]), self._times, self._columns),
                )
            self.o.data = data_array
            self.o.meta = meta
        else:  # Dataset
            data_array = xr.DataArray(
                data,
                dims=("epoch", "time", "space"),
                coords=(np.arange(data.shape[0]), self._times, self._columns),
            )
            self.o.data = xr.Dataset(
                {
                    "data": data_array,
                    "target": [
                        self._extract_target(context) for context in list_context
                    ],
                }
            )
            self.o.meta = meta

    def _extract_target(self, context):
        if self._context_key is None:
            return context
        else:
            if isinstance(context, str):
                context = json.loads(context)
            return context.get(self._context_key)

    def _valid_port(self, port):
        """Checks that the port has valid meta and data."""
        if port.data is None or port.data.empty:
            return False
        if "epoch" not in port.meta:
            return False
        if port.data.shape[0] != self._num_times:
            if self._reporting == "error":
                raise WorkerInterrupt(
                    f"Received an epoch with {port.data.shape[0]} "
                    f"samples instead of {self._num_times}."
                )
            elif self._reporting == "warn":
                self.logger.warning(
                    f"Received an epoch with {port.data.shape[0]} "
                    f"samples instead of {self._num_times}. "
                    f"Skipping."
                )
                return False
            else:  # reporting is None
                # be cool
                return False
        return True
