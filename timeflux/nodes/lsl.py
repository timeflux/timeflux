"""timeflux.nodes.hdf5: HDF5 nodes"""

import pandas as pd
import numpy as np
import uuid
from pylsl import StreamInfo, StreamOutlet, StreamInlet, resolve_stream, pylsl
from time import time
from timeflux.core.node import Node
import logging

class Send(Node):

    """Send to a LSL stream."""

    _dtypes = {
        'double64': np.number,
        'string': np.object
    }

    def __init__(self, name, stream_type='Signal', channel_format='double64', rate=0.0, source=None):
        """
        Currently, only double64 and string formats are supported
        """
        if not source:
            source = str(uuid.uuid4())
        self._name = name
        self._type = stream_type
        self._format = channel_format
        self._rate = rate
        self._source = source
        self._outlet = None

    def update(self):
        if isinstance(self.i.data, pd.core.frame.DataFrame):
            if not self._outlet:
                labels = list(self.i.data.select_dtypes(include=[self._dtypes[self._format]]))
                info = StreamInfo(self._name, self._type, len(labels), self._rate, self._format, self._source)
                channels = info.desc().append_child('channels')
                for label in labels:
                    if not isinstance('string', type(label)):
                        label = str(label)
                    channels.append_child('channel').append_child_value('label', label)
                self._outlet = StreamOutlet(info)
            values = self.i.data.select_dtypes(include=[self._dtypes[self._format]]).values
            stamps = self.i.data.index.values.astype(np.float64)
            for row, stamp in zip(values, stamps):
                self._outlet.push_sample(row, stamp)


class Receive(Node):

    """Receive from a LSL stream."""

    def __init__(self, name, unit='ns', offset_correction=False, channels=None):
        self._name = name
        self._inlet = None
        self._labels = None
        self._unit = unit
        self._offset_correction = offset_correction
        self._channels = channels
        if self._offset_correction:
            self._offset = pd.Timestamp(time(), unit='s') - pd.Timestamp(pylsl.local_clock(), unit='s')

    def update(self):
        if not self._inlet:
            logging.debug('Resolving stream: ' + self._name)
            streams = resolve_stream('name', self._name)
            logging.debug('Stream acquired')
            self._inlet = StreamInlet(streams[0])
            if isinstance(self._channels, list):
                self._labels = self._channels
            else:
                info = self._inlet.info()
                description = info.desc()
                channel = description.child('channels').first_child()
                self._labels = [channel.child_value('label')]
                for _ in range(info.channel_count() - 1):
                    channel = channel.next_sibling()
                    self._labels.append(channel.child_value('label'))
        if self._inlet:
            values, stamps = self._inlet.pull_chunk()
            if stamps:
                stamps = pd.to_datetime(stamps, format=None, unit=self._unit)
                if self._offset_correction:
                    stamps += self._offset
            self.o.set(values, stamps, self._labels)
