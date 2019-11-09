import time
from timeflux.core.node import Node
from timeflux.core.exceptions import WorkerInterrupt

class Test(Node):

    def __init__(self, interrupt_init=None):
        1 / 0

    def update(self):
        1 / 0

    def terminate(self):
        self.logger.debug('Cleaning up')
        time.sleep(1)
        self.logger.debug('Done')