"""timeflux.nodes.safeguards: Development safeguard nodes """

import datetime
import logging
import warnings

import numpy as np
import pandas as pd

from timeflux.core.exceptions import WorkerInterrupt
from timeflux.core.node import Node
from timeflux.helpers.clock import now

logger = logging.getLogger(__name__)


class SafeguardMixin:
    """Mixin for a configurable action-dependent problem management function"""

    # Note: Because timeflux does not call super in the Node constructor,
    # any mixin class *MUST* be declared before the Node class.
    # Otherwise, the mixin constructor is never called.
    # With would not be necessary if Node.__init__ had a super().__init__ call
    # However, there is one valid reason to keep Node as the last element of
    # method resolution order (MRO): Node redefines __getattr__, so it catches
    # any attribute lookup that were not in any of the mixins, without hiding
    # any methods on those mixins.

    def __init__(self, action='log', exception=None):
        super().__init__()

        if action not in ('log', 'warn', 'error', 'ignore', 'events'):
            raise ValueError(f'Unknown action "{action}" for a SafeguardMixin class')
        self._action = action

        self._exc = exception or WorkerInterrupt
        if not issubclass(self._exc, Exception):
            raise ValueError('Invalid exception type')

    @property
    def action(self):
        return self._action

    def problem(self, message, details):
        message_long = f'{message}\nDetails:\n{details}'
        message_oneline = message_long.replace('\n', ' ')
        if self.action == 'log':
            logger_ = getattr(self, 'logger', logger)
            logger_.warning(message_oneline)
        elif self.action == 'warn':
            warnings.warn(message_long, UserWarning, stacklevel=2)
        elif self.action == 'error':
            raise self._exc(message)
        elif self.action == 'events':
            self.o.data = pd.DataFrame(data=[], columns=['label'], index=now())
        elif self.action == 'ignore':
            pass


class OrderedIndex(SafeguardMixin, Node):
    """Verifies that all node inputs have a monotonic index"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # self.memo = dict()

    def update(self):
        if self.ports is None:
            return

        for name, _, port in self.iterate('i*'):
            self._check_order(name, port.data)

    def _check_order(self, name, data):
        if data is None or data.empty:
            return
        if not data.index.is_monotonic:
            idx = np.where(np.sign(np.diff(data.index)).astype(int) == -1)[0] + 1
            series = pd.Series(data='', index=data.index, name='problem')
            series[idx] = '<<<<<< HERE'
            fragments = []
            for i in idx:
                begin, end = max(i - 1, 0), min(i + 2, data.shape[0])
                fragments.append(series.iloc[begin:end].to_string())
            details = '\n...\n'.join(fragments)
            self.problem(f'Index is not monotonic for input {name}', details)

        # TODO: save the last index, which should also be checked with respect
        #       to the first new sample


class RealTime(SafeguardMixin, Node):
    """Verifies that all node inputs have real-time data"""

    def __init__(self, max_delay=1, relative=False, check_realtime=True, check_no_data=False, **kwargs):
        super().__init__(**kwargs)
        self._max_delay = max_delay
        self._relative = relative
        self._check_realtime = check_realtime
        self._check_no_data = check_no_data
        self._last_index = dict()
        self._offsets = dict()
        self._count = 0

    def update(self):
        now = datetime.datetime.utcnow()
        for name, _, port in self.iterate():
            if port.data is None or port.data.empty:
                if self._check_no_data:
                    self._check_delay_no_data(name)
            else:
                self._init_offsets(name, now, port.data)
                if self._check_realtime:
                    self._check_delay(name, port.data)

    def _init_offsets(self, name, now, data):
        if not self._relative:
            return
        if name not in self._offsets:
            self._offsets[name] = (now - data.index.max()).total_seconds()
            self.logger.info('Offset for %s is %s', name, self._offsets[name])

    def _check_delay_no_data(self, name):
        now = datetime.datetime.utcnow()
        if name not in self._last_index:
            return
        last = self._last_index[name]
        diff_seconds = (now - last).total_seconds() - self._offsets.get(name, 0)

        if np.abs(diff_seconds) > self._max_delay:
            details = (
                f'Last data timestamp: {last} '
                f' (offset-corrected): {last + datetime.timedelta(seconds=self._offsets.get(name, 0))} '
                f'Current time: {now} '
                f'Difference: {diff_seconds} secs '
                f'Sample count: {self._count}'
            )
            self.problem(f'Input {name} is lagging behind (no new data received)', details)

    def _check_delay(self, name, data):
        now = datetime.datetime.utcnow()
        last = data.index[-1]
        diff_seconds = (now - last).total_seconds() - self._offsets.get(name, 0)
        self._count += data.shape[0]

        if np.abs(diff_seconds) > self._max_delay:
            details = (
                f'Last data timestamp: {last} '
                f' (offset-corrected): {last + datetime.timedelta(seconds=self._offsets.get(name, 0))} '
                f'Current time: {now} '
                f'Difference: {diff_seconds} secs '
                f'Sample count: {self._count}'
            )
            self.problem(f'Input {name} cannot reach real-time', details)

        self._last_index[name] = last
