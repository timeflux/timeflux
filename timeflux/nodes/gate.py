"""Gate node that resume or stop the streaming data """

from itertools import cycle

from timeflux.core.node import Node
import xarray as xr


class Gate(Node):
    """Data-gate based on event triggers.

    This node cuts off or puts through data depending on event triggers
    It has 3 operating mode/status:
    - silent: the node waits for an opening trigger in the events and returns nothing
    - opened: the node waits for a closing trigger in the events and free pass the data
    - closed: the node has just received a closing trigger, so free pass the data and resets its state.


    It continuously iterates over events data to update its operating mode.

    Attributes:
        i (Port): Default data input, expects DataFrame or or XArray.

        i_events (Port): Event input, expects DataFrame.
        o (Port): Default output, provides DataFrame or XArray and meta.

    Args:
        event_opens (string): The marker name on which the gate open.s
        event_closes (string): The marker name on which the the gate closes.
        event_label (string): The column to match for event_trigger.

    Todo: allow for multiple input ports.

    """

    def __init__(self, event_opens, event_closes, event_label="label", truncate=False):
        self._event_label = event_label
        self._event_opens = event_opens
        self._event_closes = event_closes
        self._truncate = truncate
        self._reset()

    def update(self):
        # Iter over events to match the opening/closing trigger.

        if self.i_events.ready():
            for index, row in self.i_events.data.iterrows():
                if row[self._event_label] == self._trigger:
                    self.logger.debug(f"Gate received {self._trigger}.")
                    self._next()
                    # keep track of opening/closing of the gate
                    self._times.append(index)
        self._update()

    def _update(self):
        # if the gate is either open or closing, truncate and forward the data,
        # if the gate is closing, reset status and trigger iterator,
        # else (gate is closed), return

        if self._status == "open":
            self.o = self.i
            self.o.meta["gate_status"] = self._status
            if self._truncate and self.o.ready():
                if isinstance(self.o.data, xr.DataArray):
                    self.o.data = self.o.data.sel(
                        {"time": slice(self._times[0], self.o.data.time[-1])}
                    )
                else:  # isinstance(self.o.data,pd.DataFrame)
                    # truncate the data after opening time
                    self.o.data = self.o.data[self._times[0] :]
        elif self._status == "closing":
            self.o = self.i
            self.o.meta.update({"gate_status": "closed", "gate_times": self._times})
            # truncate the data between opening and closing times
            if self._truncate and self.o.ready():
                if isinstance(self.o.data, xr.DataArray):
                    self.o.data = self.o.data.sel(
                        {"time": slice(self._times[0], self._times[1])}
                    )
                else:  # isinstance(self.o.data,pd.DataFrame)
                    self.o.data = self.o.data[self._times[0] : self._times[1]]
            self._reset()
        else:  # self._status == 'closed'
            self.o.data = None
            self.o.meta = {"gate_status": "closed"}

    def _next(self):
        # iterates trigger (expected event) and status (defining the mode of the node)
        self._status = next(self._status_iterator)
        self._trigger = next(self._trigger_iterator)
        self.logger.debug(f"Gate is {self._status}.")

    def _reset(self):
        # Reset iterator states
        self._times = []
        self._trigger_iterator = cycle([self._event_opens, self._event_closes])
        self._status_iterator = cycle(["closed", "open", "closing"])
        # initialize trigger and status
        self._next()
