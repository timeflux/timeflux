import pandas as pd
from timeflux.core.node import Node


class Epoch(Node):

    """Event-triggered epoching.

    This node continuously buffers a small amount of data (of a duration of ``before`` seconds) from the default input stream.
    When it detects a marker matching the ``event_trigger`` in the ``event_label`` column of the event input stream, it starts accumulating data for ``after`` seconds.
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
        event_label (string): The column where ``event_trigger`` is expected.
        event_data (string, None): The column where metadata can be found.

    Example:
        .. literalinclude:: /../test/graphs/epoch.yaml
           :language: yaml

    """

    def __init__(self,
                 event_trigger,
                 before=.2,
                 after=.6,
                 event_label='label',
                 event_data='data'):

        self._event_trigger = event_trigger
        self._event_label = event_label
        self._event_data = event_data
        self._before = pd.Timedelta(seconds=before)
        self._after = pd.Timedelta(seconds=after)
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
        if self.i_events.data is not None:
            if not self.i_events.data.empty:
                matches = self.i_events.data[self.i_events.data[self._event_label] == self._event_trigger]
                if not matches.empty:
                    for index, row in matches.iterrows():
                        # Start a new epoch
                        low = index - self._before
                        high = index + self._after
                        self._epochs.append({
                            'data': self._buffer[low:high],
                            'meta': {
                                'onset': index,
                                'context': row[self._event_data] if self._event_data is not None else None
                            }
                        })

        # Trim main buffer
        if self._buffer is not None:
            low = self._buffer.index[-1] - self._before
            self._buffer = self._buffer[low:]

        # Update epochs
        if self._epochs:
            if self.i.data is not None:
                if not self.i.data.empty:
                    complete = 0
                    for epoch in self._epochs:
                        low = epoch['meta']['onset'] - self._before
                        high = epoch['meta']['onset'] + self._after
                        last = self.i.data.index[-1]
                        # Append
                        mask = (self.i.data.index >= low) & (self.i.data.index <= high)
                        epoch['data'] = epoch['data'].append(self.i.data[mask])
                        # Send if we have enough data
                        if last >= high:
                            o = getattr(self, 'o_' + str(complete))
                            o.data = epoch['data']
                            o.meta = {'epoch': epoch['meta']}
                            complete += 1
                    if complete > 0:
                        del self._epochs[:complete] # Unqueue
                        self.o = self.o_0 # Bind default output to the first epoch
