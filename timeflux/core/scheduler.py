"""timeflux.core.schedule: run nodes"""

import logging
from time import time, sleep
from copy import deepcopy
from timeflux.core.registry import Registry


class Scheduler:

    def __init__(self, path, nodes, rate):
        self.logger = logging.getLogger(__name__)
        self._path = path
        self._nodes = nodes
        self._rate = rate


    def run(self):
        while True:
            start = time()
            Registry.cycle_start = start
            self.next()
            duration = time() - start
            if self._rate > 0:
                max_duration = 1. / self._rate
                if duration > max_duration:
                    self.logger.debug('Congestion')
                sleep(max(0, max_duration - duration))


    def next(self):
        for step in self._path:
            # Clear ports
            self._nodes[step['node']].clear()
            # Update inputs from predecessor outputs
            if step['predecessors']:
                for predecessor in step['predecessors']:
                    # Get a generator so dynamic ports are expanded
                    src_ports = self._nodes[predecessor['node']].iterate(predecessor['src_port'])
                    for name, suffix, src_port in src_ports:
                        if suffix: suffix = '_' + suffix
                        data = src_port.data
                        meta = src_port.meta
                        if predecessor['copy']:
                            if data is not None:
                                data = data.copy(deep=True)
                            if meta is not None:
                                meta = deepcopy(meta)
                        dst_port = getattr(self._nodes[step['node']], predecessor['dst_port'] + suffix)
                        dst_port.data = data
                        dst_port.meta = meta
            # Update node
            try:
                self._nodes[step['node']].update()
            except Exception as e:
                inputs = self._nodes[step['node']].iterate('i*')
                input_log = ''.join('\n\n%s: %s\n\n' % (name, port.data) for name, suffix, port in inputs)
                self.logger.warn("Node {} failed at update with {} '{}'. {}".format(step['node'], type(e), e.args[0], input_log))
                message = "'{}' (raised while updating timeflux node {})".format(e.args[0], step['node'])
                args = list(e.args)
                args[0] = message
                e.args = tuple(args)
                raise e from None





    def terminate(self):
        for step in self._path:
            self._nodes[step['node']].terminate()
