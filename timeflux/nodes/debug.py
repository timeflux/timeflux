"""Simple debugging nodes"""

import csv
import pandas as pd
from datetime import timezone
from timeflux.core.node import Node
from timeflux.helpers.clock import now


class Display(Node):
    """Display input."""

    def __init__(self, meta=False, data=True):
        self._meta = meta
        self._data = data

    def update(self):
        if self.i.ready() and self._data:
            self.logger.debug("\n %s" % self.i.data)
        if self.i.meta and self._meta:
            self.logger.debug("\n %s" % self.i.meta)


class Dump(Node):
    """Dump to CSV."""

    def __init__(self, fname="/tmp/dump.csv"):
        self.writer = csv.writer(open(fname, "a"))

    def update(self):
        if self.i.ready():
            self.i.data["index"] = self.i.data.index.values.astype("float64") / 1e9
            self.writer.writerows(self.i.data.values.tolist())


class Latency(Node):
    """Mesure Latency."""

    def update(self):
        if self.i.ready():
            now = pd.Timestamp.now(timezone.utc)
            indices = self.i.data.index.tz_localize(timezone.utc)
            latencies = list((now - indices).total_seconds())
            self.logger.debug(
                f"{latencies[0]} ... {latencies[-1]} ({len(latencies)} datapoints)"
            )
