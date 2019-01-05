"""timeflux.core.schedule: run nodes"""

import sys
import signal
import logging
from time import time, sleep
from copy import deepcopy
from timeflux.core.exceptions import WorkerInterrupt
from timeflux.core.registry import Registry

class Scheduler:

    def __init__(self, path, nodes, rate):
        self._path = path
        self._nodes = nodes
        self._rate = rate

    def run(self):
        try:
            while True:
                start = time()
                Registry.cycle_start = start
                self.next()
                duration = time() - start
                if self._rate > 0:
                    max_duration = 1. / self._rate
                    if duration > max_duration:
                        logging.debug('Congestion')
                    sleep(max(0, max_duration - duration))
        except WorkerInterrupt as error:
            logging.debug(error)
        logging.info('Terminating')

    def next(self):
        for step in self._path:
            # Clear ports
            self._nodes[step['node']].clear()
            # Update inputs from predecessor outputs
            if step['predecessors']:
                for predecessor in step['predecessors']:
                    data = getattr(self._nodes[predecessor['node']], predecessor['src_port']).data
                    meta = getattr(self._nodes[predecessor['node']], predecessor['src_port']).meta
                    if predecessor['copy']:
                        if data is not None:
                            data = data.copy(deep=True)
                        if meta is not None:
                            meta = deepcopy(meta)
                    port = getattr(self._nodes[step['node']], predecessor['dst_port'])
                    port.data = data
                    port.meta = meta
            # Update node
            self._nodes[step['node']].update()
