import time
from timeflux.core.node import Node
from timeflux.core.exceptions import WorkerInterrupt

class Test(Node):

    def __init__(self, interrupt=None):
        self._count = 0
        self._interrupt = interrupt

    def update(self):
        self._count += 1
        self.logger.debug('Doing things')
        if self._interrupt == self._count:
            raise WorkerInterrupt('Interrupting')

    def terminate(self):
        self.logger.debug('Cleaning up')
        time.sleep(1)
        self.logger.debug('Done')