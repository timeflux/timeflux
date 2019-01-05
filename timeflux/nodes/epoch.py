import pandas as pd
from timeflux.core.node import Node


class Epoch(Node):

    """Event-triggered epoching

    This node continuously buffers a small amount of data (of a duration of ``before`` seconds) on the default input stream.
    When it detects a marker matching the ``event_trigger`` in the ``event_label`` column of the event input, it starts accumulating data for ``after`` seconds.
    It then sends the epoched data in the default output stream, and set the ``epoch`` field of ``o.meta`` to a dictionary containing the triggering marker and optional event data.

    Attributes:
        i (Port): Default data input, expects DataFrame.
        i_events (Port): Event input, expects DataFrame.
        o (Port): Default output, provides DataFrame and meta.

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
        """
        Args:
            event_trigger (string): The marker name.
            before (float): Length before onset, in seconds.
            after (float): Length after onset, in seconds.
            event_label (string): The column to match for event_trigger.
            event_data (string, None): The column where meta-data is stored.
        """

        self._event_trigger = event_trigger
        self._event_label = event_label
        self._event_data = event_data
        self._before = pd.Timedelta(seconds=before)
        self._after = pd.Timedelta(seconds=after)
        self._reset()

    def update(self):

        # Append data
        if self.i.data is not None:
            if not self.i.data.empty:
                if self._buffer is None:
                    self._buffer = self.i.data
                else:
                    self._buffer = self._buffer.append(self.i.data)

        # Detect onset
        if self.i_events.data is not None:
            if not self.i_events.data.empty:
                matches = self.i_events.data[self.i_events.data[
                    self._event_label] == self._event_trigger]
                if not matches.empty:
                    self._onset = matches.index[0]
                    self._meta = {
                        'epoch': {
                            'onset':
                            self._onset,
                            'context':
                            matches.iloc[0][self._event_data]
                            if self._event_data is not None else None
                        }
                    }

        # Trim
        last = self._buffer.index[-1]
        if self._onset is None:
            high = last
            low = high - self._before
        else:
            high = self._onset + self._after
            low = self._onset - self._before
        self._buffer = self._buffer = self._buffer[low:high]

        # Send if we have enough data
        if self._onset is not None:
            if last - self._onset >= self._after:
                self.o.data = self._buffer
                self.o.meta = self._meta
                self._reset()

    def _reset(self):
        self._onset = None
        self._meta = None
        self._buffer = None
