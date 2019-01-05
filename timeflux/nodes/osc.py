"""timeflux.nodes.osc: Simple OSC client and server"""

import logging
import pandas as pd
from pythonosc import dispatcher
from pythonosc import osc_server
from pythonosc import udp_client
from timeflux.core.node import Node

class Server(Node):

    """A simple OSC server with no forwarding capabilities. Useful for debugging."""

    def __init__(self, addresses=[], ip='127.0.0.1', port=5005):
        if not addresses or not isinstance(addresses, list):
            raise ValueError('You must provide a list of addresses.')
        d = dispatcher.Dispatcher()
        for address in addresses:
            d.map(address, print)
        self._server = osc_server.ThreadingOSCUDPServer((ip, port), d)

    def update(self):
        self._server.serve_forever()


class Client(Node):

    """A simple OSC client."""

    def __init__(self, address='', ip='127.0.0.1', port=5005):
        if not address or not isinstance(address, str):
            raise ValueError('You must provide an address.')
        self._address = address
        self._client = udp_client.SimpleUDPClient(ip, port)

    def update(self):
        if self.i.data is not None:
            for row in self.i.data.itertuples(index=False):
                self._client.send_message(self._address, row)
