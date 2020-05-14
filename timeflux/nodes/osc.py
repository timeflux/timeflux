"""timeflux.nodes.osc: Simple OSC client and server"""

import pandas as pd
from threading import Thread, Lock
from pythonosc.dispatcher import Dispatcher
from pythonosc.osc_server import BlockingOSCUDPServer
from pythonosc.udp_client import SimpleUDPClient
from timeflux.helpers.clock import now
from timeflux.core.node import Node


class Server(Node):

    """A simple OSC server."""

    def __init__(self, addresses=[], ip="127.0.0.1", port=5005):
        self._server = None
        self._data = {}
        self._lock = Lock()
        if not addresses or not isinstance(addresses, list):
            raise ValueError("You must provide a list of addresses.")
        dispatcher = Dispatcher()
        for address in addresses:
            self._data[self._address_to_port(address)] = {"timestamps": [], "rows": []}
            dispatcher.map(address, self._handler)
        self._server = BlockingOSCUDPServer((ip, port), dispatcher)
        Thread(target=self._server.serve_forever).start()

    def update(self):
        with self._lock:
            for port, data in self._data.items():
                if data["rows"]:
                    getattr(self, port).set(data["rows"], data["timestamps"])
                    self._data[port] = {"timestamps": [], "rows": []}

    def terminate(self):
        if self._server:
            self._server.shutdown()

    def _handler(self, address, *args):
        time = now()
        port = self._address_to_port(address)
        values = list(args)
        with self._lock:
            self._data[port]["rows"].append(values)
            self._data[port]["timestamps"].append(time)

    def _address_to_port(self, address):
        address = "/" + address if not address.startswith("/") else address
        return "o" + address.replace("/", "_")


class Client(Node):

    """A simple OSC client."""

    def __init__(self, address="", ip="127.0.0.1", port=5005):
        if not address or not isinstance(address, str):
            raise ValueError("You must provide an address.")
        self._address = address
        self._client = SimpleUDPClient(ip, port)

    def update(self):
        if self.i.data is not None:
            for row in self.i.data.itertuples(index=False):
                self._client.send_message(self._address, row)
