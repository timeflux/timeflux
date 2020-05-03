"""timeflux.nodes.index: Node to handle DataFrame index """

import numpy as np
import pandas as pd

from timeflux.core.node import Node


class MakeMonotonic(Node):
    """ Make index monotonic
    """

    def __init__(self):
        self._last = None

    def update(self):
        if not self.i.ready():
            return

        self.o = self.i

        if not self.i.data.index.is_monotonic:
            # index is not monotonic
            backward_step_index = np.where(np.diff(self.i.data.index) < np.timedelta64(0, 's'))[0] + 1
            if self._last is None:
                # node not ready (very first chunk)
                self.drop_backward_steps(backward_step_index)
            if len(backward_step_index) > 1:
                self.logger.warning('More than one backward step not supported.'
                                    'Will remove backward stepping rows.  ')
                self.drop_backward_steps(backward_step_index)

            else:
                monotonic_chunks = np.split(self.i.data, backward_step_index)
                backward_duration = (monotonic_chunks[1].index[0] - monotonic_chunks[0].index[-1]).total_seconds()
                self.logger.info(f'Received data with non-monotonic index of {backward_duration} seconds '
                                 'making it monotonic.  ')
                if self._last > monotonic_chunks[1].index[0]:
                    self.logger.warning('Backward step cannot be corrected because accumulated late is too important.')
                    self.drop_backward_steps(backward_step_index)
                else:
                    self.make_monotonic(monotonic_chunks)

        self._last = self.i.data.index.max()

    def drop_backward_steps(self, backward_step_index):
        self.o.data = self.i.data.drop(self.i.data.index[backward_step_index], axis=0)

    def make_monotonic(self, monotonic_chunks):
        start = self._last.value
        stop = monotonic_chunks[1].index[0].value
        monotonic_index = np.linspace(start, stop, len(monotonic_chunks[0]) + 1, endpoint=False, dtype='datetime64[ns]')
        monotonic_chunks[0].index = monotonic_index[1:]
        self.o.data = pd.concat(monotonic_chunks)
