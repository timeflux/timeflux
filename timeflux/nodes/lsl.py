"""Lab streaming layer nodes

    The lab streaming layer provides a set of functions to make instrument data
    accessible in real time within a lab network. From there, streams can be
    picked up by recording programs, viewing programs or custom experiment
    applications that access data streams in real time.

"""

import os
import pandas as pd
import numpy as np
import uuid
from pylsl import (
    StreamInfo,
    StreamOutlet,
    StreamInlet,
    resolve_stream,
    resolve_byprop,
    pylsl,
)
from time import time
from timeflux.core.node import Node


class Send(Node):

    """Send to a LSL stream.

    Attributes:
        i (Port): Default data input, expects DataFrame.

    Args:
        name (string): The name of the stream.
        type (string): The content type of the stream, .
        format (string): The format type for each channel. Currently, only ``double64`` and ``string`` are supported.
        rate (float): The nominal sampling rate. Set to ``0.0`` to indicate a variable sampling rate.
        source (string, None): The unique identifier for the stream. If ``None``, it will be auto-generated.
        config_path (string, None): The path to an LSL config file.

    Example:
        .. literalinclude:: /../examples/lsl.yaml
           :language: yaml

    """

    _dtypes = {"double64": np.number, "string": object}

    def __init__(
        self,
        name,
        type="Signal",
        format="double64",
        rate=0.0,
        source=None,
        config_path=None,
    ):
        if not source:
            source = str(uuid.uuid4())
        self._name = name
        self._type = type
        self._format = format
        self._rate = rate
        self._source = source
        self._outlet = None
        if config_path:
            os.environ["LSLAPICFG"] = config_path

    def update(self):
        if isinstance(self.i.data, pd.core.frame.DataFrame):
            if not self._outlet:
                labels = list(
                    self.i.data.select_dtypes(include=[self._dtypes[self._format]])
                )
                info = StreamInfo(
                    self._name,
                    self._type,
                    len(labels),
                    self._rate,
                    self._format,
                    self._source,
                )
                channels = info.desc().append_child("channels")
                for label in labels:
                    if not isinstance("string", type(label)):
                        label = str(label)
                    channels.append_child("channel").append_child_value("label", label)
                self._outlet = StreamOutlet(info)
            values = self.i.data.select_dtypes(
                include=[self._dtypes[self._format]]
            ).values
            stamps = self.i.data.index.values.astype(np.float64) / 1e9
            for row, stamp in zip(values, stamps):
                self._outlet.push_sample(row, stamp)


class Receive(Node):

    """Receive from a LSL stream.

    Attributes:
        o (Port): Default output, provides DataFrame and meta.

    Args:
        prop (string): The property to look for during stream resolution (e.g., ``name``, ``type``, ``source_id``).
        value (string): The value that the property should have (e.g., ``EEG`` for the type property).
        timeout (float): The resolution timeout, in seconds.
        channels (list, None): Override the channel names. If ``None``, the names defined in the LSL stream will be used.
        max_samples (int): The maximum number of samples to return per call.
        clocksync (bool): Perform automatic clock synchronization.
        dejitter (bool): Remove jitter from timestamps using a smoothing algorithm to the received timestamps.
        monotonize (bool): Force the timestamps to be monotonically ascending. Only makes sense if timestamps are dejittered.
        threadsafe (bool): Same inlet can be read from by multiple threads.
        config_path (string, None): The path to an LSL config file.

    Example:
        .. literalinclude:: /../examples/lsl_multiple.yaml
           :language: yaml

    """

    def __init__(
        self,
        prop="name",
        value=None,
        timeout=1.0,
        channels=None,
        max_samples=1024,
        clocksync=True,
        dejitter=False,
        monotonize=False,
        threadsafe=True,
        config_path=None,
    ):
        if not value:
            raise ValueError("Please specify a stream name or a property and value.")
        self._prop = prop
        self._value = value
        self._inlet = None
        self._labels = None
        self._channels = channels
        self._timeout = timeout
        self._max_samples = max_samples
        self._flags = 0
        self._offset = 0
        if clocksync:
            self._flags |= 1
            self._offset = time() - pylsl.local_clock()
        if dejitter:
            self._flags |= 2
        if monotonize:
            self._flags |= 4
        if threadsafe:
            self._flags |= 8
        if config_path:
            os.environ["LSLAPICFG"] = config_path

    def update(self):
        if not self._inlet:
            self.logger.debug(f"Resolving stream with {self._prop} {self._value}")
            streams = resolve_byprop(self._prop, self._value, timeout=self._timeout)
            if not streams:
                return
            self.logger.debug("Stream acquired")
            self._flags = pylsl.proc_clocksync | pylsl.proc_dejitter
            self._inlet = StreamInlet(streams[0], processing_flags=self._flags)
            info = self._inlet.info()
            self._meta = {
                "name": info.name(),
                "type": info.type(),
                "rate": info.nominal_srate(),
                "info": str(info.as_xml()).replace("\n", "").replace("\t", ""),
            }
            if isinstance(self._channels, list):
                self._labels = self._channels
            else:
                description = info.desc()
                channel = description.child("channels").first_child()
                self._labels = [channel.child_value("label")]
                for _ in range(info.channel_count() - 1):
                    channel = channel.next_sibling()
                    self._labels.append(channel.child_value("label"))
        if self._inlet:
            values, stamps = self._inlet.pull_chunk(max_samples=self._max_samples)
            if stamps:
                if self._offset != 0:
                    self._offset
                    stamps = np.array(stamps) + self._offset
                stamps = pd.to_datetime(stamps, format=None, unit="s")
                self.o.set(values, stamps, self._labels, self._meta)
