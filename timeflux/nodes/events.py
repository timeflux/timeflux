"""timeflux.nodes.events: generate random events"""

import random
import string
import json
from timeflux.core.node import Node

class Events(Node):

    def __init__(self, rows_min=1, rows_max=10, string_min=3, string_max=12, items_min=0, items_max=5, seed=None):
        """Return random integers from value_min to value_max (inclusive)"""
        self._rows_min = rows_min
        self._rows_max = rows_max
        self._string_min = string_min
        self._string_max = string_max
        self._items_min = items_min
        self._items_max = items_max
        random.seed(seed)

    def random_string(self, length):
        return ''.join(random.choice(string.ascii_letters) for m in range(length))

    def update(self):
        rows = []
        for i in range(random.randint(self._rows_min, self._rows_max)):
            row = []
            row.append(self.random_string(random.randint(self._string_min, self._string_max)))
            data = {}
            for j in range(random.randint(self._items_min, self._items_max)):
                key = self.random_string(random.randint(self._string_min, self._string_max))
                value = self.random_string(random.randint(self._string_min, self._string_max))
                data[key] = value
            row.append(json.dumps(data))
            rows.append(row)
        self.o.set(rows, names=['label', 'data'])
