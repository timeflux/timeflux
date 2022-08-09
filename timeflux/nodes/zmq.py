"""timeflux.nodes.zmq: a simple 0MQ pub/sub broker"""

import time
import pandas
import zmq
from zmq.devices import ThreadProxy
from timeflux.core.node import Node
from timeflux.core.exceptions import WorkerInterrupt
from timeflux.core.io import Port
import timeflux.core.message


class Broker(Node):

    """Must run in its own graph."""

    def __init__(
        self, address_in="tcp://127.0.0.1:5559", address_out="tcp://127.0.0.1:5560"
    ):
        """
        Initialize frontend and backend.
        If used on a LAN, bind to tcp://*:5559 and tcp://*:5560 instead of localhost.
        """
        try:
            context = zmq.Context.instance()
            self._frontend = context.socket(zmq.XSUB)
            self._frontend.bind(address_in)
            self._backend = context.socket(zmq.XPUB)
            self._backend.bind(address_out)
        except zmq.ZMQError as e:
            self.logger.error(e)

    def update(self):
        """Start a blocking proxy."""
        zmq.proxy(self._frontend, self._backend)


class BrokerMonitored(Node):

    """
    Run a monitored pub/sub proxy.
    Will shut itself down after [timeout] seconds if no data is received.
    Useful for unit testing and replays.
    """

    _last_event = time.time()

    def __init__(
        self,
        address_in="tcp://127.0.0.1:5559",
        address_out="tcp://127.0.0.1:5560",
        timeout=5,
    ):

        self._timeout = timeout

        try:

            # Capture
            address_monitor = "inproc://monitor"
            context = zmq.Context.instance()
            self._monitor = context.socket(zmq.PULL)
            self._monitor.bind(address_monitor)

            # Proxy
            proxy = ThreadProxy(zmq.XSUB, zmq.XPUB, zmq.PUSH)
            proxy.bind_in(address_in)
            proxy.bind_out(address_out)
            proxy.connect_mon(address_monitor)
            # proxy.setsockopt_mon(zmq.CONFLATE, True) # Do not clutter the network
            proxy.start()

        except zmq.ZMQError as error:
            self.logger.error(error)

    def update(self):
        """Monitor proxy"""
        if self._timeout == 0:
            pass
        now = time.time()
        count = 0
        try:
            while True:
                self._monitor.recv_multipart(zmq.NOBLOCK, copy=False)
                self._last_event = now
                count += 1
        except zmq.ZMQError:
            if count > 0:
                self.logger.debug("Received %d messages", count)
            if (now - self._last_event) > self._timeout:
                raise WorkerInterrupt("No data after %d seconds" % self._timeout)


class BrokerLVC(Node):
    """A monitored pub/sub broker with last value caching."""

    def __init__(
        self,
        address_in="tcp://127.0.0.1:5559",
        address_out="tcp://127.0.0.1:5560",
        timeout=1000,
    ):
        self._timeout = timeout
        try:
            context = zmq.Context.instance()
            self._frontend = context.socket(zmq.SUB)
            self._frontend.setsockopt(zmq.SUBSCRIBE, b"")
            self._frontend.bind(address_in)
            self._backend = context.socket(zmq.XPUB)
            self._backend.setsockopt(zmq.XPUB_VERBOSE, True)
            self._backend.bind(address_out)
            self._poller = zmq.Poller()
            self._poller.register(self._frontend, zmq.POLLIN)
            self._poller.register(self._backend, zmq.POLLIN)
        except zmq.ZMQError as error:
            self.logger.error(error)

    def update(self):
        """Main poll loop."""
        cache = {}
        while True:
            events = dict(self._poller.poll(self._timeout))
            # Any new topic data we cache and then forward
            if self._frontend in events:
                message = self._frontend.recv_multipart()
                topic, current = message
                cache[topic] = current
                self._backend.send_multipart(message)
            # handle subscriptions
            # When we get a new subscription we pull data from the cache:
            if self._backend in events:
                event = self._backend.recv()
                # print(event)
                # Event is one byte 0=unsub or 1=sub, followed by topic
                # if event[0] == b'\x01':
                if event[0] == 1:
                    topic = event[1:]
                    if topic in cache:
                        self.logger.debug(
                            "Sending cached topic %s", topic.decode("utf-8")
                        )
                        self._backend.send_multipart([topic, cache[topic]])


class Pub(Node):
    def __init__(
        self, topic, address="tcp://127.0.0.1:5559", serializer="pickle", wait=0
    ):
        """Create a publisher"""
        self._topic = topic.encode("utf-8")
        self._serializer = getattr(timeflux.core.message, serializer + "_serialize")
        try:
            context = zmq.Context.instance()
            self._socket = context.socket(zmq.PUB)
            self._socket.setsockopt(zmq.LINGER, 0)
            self._socket.connect(address)
        except zmq.ZMQError as e:
            self.logger.error(e)

        # Quick fix to the slow joiner syndrome
        # TODO: remove when Last Value Caching is implemented
        # Wait for subscribers to connect
        # http://zguide.zeromq.org/page%3aall#Getting-the-Message-Out
        # http://zguide.zeromq.org/page%3aall#Node-Coordination
        # http://zguide.zeromq.org/page%3aall#Last-Value-Caching
        # https://stackoverflow.com/questions/30864145/zmq-no-subscription-message-on-xpub-socket-for-multiple-subscribers-last-value
        time.sleep(wait)

    def update(self):
        for name, suffix, port in self.iterate("i*"):
            if port.ready() or port.meta:
                if not suffix:
                    topic = self._topic
                else:
                    topic = self._topic + suffix.encode("utf-8")
                try:
                    if not port.ready():
                        port.data = None  # make sure we do not send corrupted data
                    self._socket.send_serialized(
                        [topic, port.data, port.meta], self._serializer
                    )
                except zmq.ZMQError as e:
                    self.logger.error(e)


class Sub(Node):
    def __init__(
        self, topics=[""], address="tcp://127.0.0.1:5560", deserializer="pickle"
    ):
        """Create a subscriber"""
        try:
            context = zmq.Context.instance()
            self._socket = context.socket(zmq.SUB)
            self._socket.connect(address)
            for topic in topics:
                self._socket.setsockopt(zmq.SUBSCRIBE, topic.encode("utf-8"))
                if topic:
                    if not topic.isidentifier():
                        raise ValueError("Invalid topic name: %s" % topic)
            self._deserializer = getattr(
                timeflux.core.message, deserializer + "_deserialize"
            )
        except zmq.ZMQError as e:
            self.logger.error(e)

    def update(self):
        self._chunks = {}
        try:
            while True:
                [topic, data, meta] = self._socket.recv_serialized(
                    self._deserializer, zmq.NOBLOCK
                )
                if not topic in self._chunks:
                    self._chunks[topic] = {"data": [], "meta": {}}
                self._append_data(topic, data)
                self._append_meta(topic, meta)
        except zmq.ZMQError:
            pass  # No more data
        self._update_ports()

    def _append_data(self, topic, data):
        if data is not None:
            self._chunks[topic]["data"].append(data)

    def _append_meta(self, topic, meta):
        if meta:
            self._chunks[topic]["meta"].update(meta)

    def _update_ports(self):
        for topic in self._chunks.keys():
            if len(self._chunks[topic]["data"]) == 0:
                data = None
            elif len(self._chunks[topic]["data"]) == 1:
                data = self._chunks[topic]["data"][0]
            else:
                data = pandas.concat(self._chunks[topic]["data"])
            meta = self._chunks[topic]["meta"]
            self._update_port(topic, data, meta)

    def _update_port(self, topic, data, meta):
        getattr(self, "o_" + topic).data = data
        getattr(self, "o_" + topic).meta = meta
