"""timeflux.core.sync: time synchronisation based on NTP"""

import socket
import logging
import re
import time
import numpy as np
from scipy import stats


class Server:

    _buffer_size = 128

    def __init__(self, host="", port=12300, now=time.perf_counter):
        self.logger = logging.getLogger(__name__)
        self._host = host
        self._port = port
        self._sock = None
        self.now = now

    def start(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.settimeout(None)  # blocking mode
        self._sock.bind((self._host, self._port))
        self.logger.info("Sync server listening on %s:%d", self._host, self._port)
        while True:
            try:
                data, address = self._sock.recvfrom(self._buffer_size)
                t1 = self.now()
                t2 = self.now()
                data += b",%.6f,%.9f" % (t1, t2)
                l = self._sock.sendto(data, address)
            except:
                pass

    def stop(self):
        self._sock.close()


class Client:

    _buffer_size = 128

    def __init__(
        self, host="localhost", port=12300, rounds=600, timeout=1, now=time.perf_counter
    ):
        self.logger = logging.getLogger(__name__)
        self._host = host
        self._port = port
        self._rounds = rounds
        self._timeout = timeout
        self.now = now
        self._sock = None
        self.offset_local = time.time() - now()
        self.offset_remote = None

    def sync(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.settimeout(self._timeout)
        t = [[], [], [], []]
        reg = re.compile(b"(.*),(.*),(.*)")
        self.logger.info("Syncing")
        i = 0
        while i < self._rounds:
            try:
                t0 = b"%.9f" % self.now()
                self._sock.sendto(t0, (self._host, self._port))
                data, address = self._sock.recvfrom(self._buffer_size)
                t3 = self.now()
                r = reg.findall(data)[0]
                if r[0] == t0:  # make sure we have a matching UDP packet
                    r = np.float64(r)
                    for j in range(3):
                        t[j].append(r[j])
                    t[3].append(np.float64(t3))
                    i += 1
            except socket.timeout:
                continue
            progress = "Progress: %.2f%%" % (i * 100 / self._rounds)
            print(progress, end="\r", flush=True)
        self._sock.close()
        t = np.array(t)
        offset = ((t[1] - t[0]) + (t[2] - t[3])) / 2
        delay = (t[3] - t[0]) - (t[2] - t[1])
        _, offset, _, _ = stats.theilslopes(offset, delay)
        self.offset_remote = offset
        self.logger.info("Offset: %f", offset)
        return offset

    def stop(self):
        self._sock.close()
