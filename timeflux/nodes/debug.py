"""timeflux.nodes.debug: dubugging"""

import csv
import pandas as pd
from timeflux.core.node import Node

class Display(Node):

    """Display input."""

    def update(self):
        if self.i.ready():
            self.logger.debug('\n %s' % self.i.data)


class Dump(Node):

    """Dump to CSV."""

    def __init__(self, fname='/tmp/dump.csv'):
        self.writer = csv.writer(open(fname, 'a'))

    def update(self):
        if self.i.ready():
            self.i.data['index'] = self.i.data.index
            self.i.data['index'] = pd.to_timedelta(self.i.data['index']).dt.total_seconds()
            self.writer.writerows(self.i.data.values.tolist())
