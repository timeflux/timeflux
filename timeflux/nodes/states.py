"""Generate random events"""

import copy
import datetime
import numpy as np
import pandas as pd

from timeflux.core.node import Node


class States(Node):
    """A state machine that transitions between states when triggered by events.

    The node's main output is the state at a series of times. The times always come from the main input if there is one.

    At any time the node is always in one of the states. The node moves between the states in response to events it
    receives. The events are interpreted as instructions to perform a certain transition, and are ignored if they are
    not valid for the current state of the node.

    Named transitions between states can be specified. For every state a default transition from any other state is
    provided, unless prevented by an explicitly specified transition.

    The events are found in i_events if it is provided, otherwise in the default input. If an event matches the name of
    a transition, it triggers that transition if the transition is appropriate for the current state.

    States can persist for at least a certain number of seconds (during which events will be ignored) and
    can also automatically transition after a fixed number of seconds.


    Args:
        states (list): The names of the states.
        transitions (dict): The transitions between states. The dictionary keys are the names of the transitions.
            Each transition is itself a dictionary which must have a 'to' key indicating the state it transitions to.
            It may have a 'from' key indicating the states this transition may transition from (if not specified, all are assumed).
        properties (dict): Dictionary whose keys are state names. Each item is a dictionary of properties for that state.
            'min_seconds' is the minimum amount of time that must be spent in the state before transitioning to any other.
            'auto_transition' is the transition to automatically apply after 'auto_transition_seconds'.
        initial (string): The state to start in. Defaults to first item in states.
        event_label (string): The column in i or i_events to look for transitions in.
        reflexive_transitions (bool): Whether a state can transition from itself to itself. Default: False.

    Attributes:
        i (Port): (optional) Default input, expects a pandas.DataFrame with timestamp index. The events are taken from this if there is no i_events.
        i_events (Port): (optional) Events input, expects a pandas.DataFrame with timestamp index.
        o (Port): Default output, provides a pandas.DataFrame with index from i and a column 'state' that indicates the state at every index.

    Examples:

        .. code-block:: yaml

           graphs:
              - nodes:
                - id: material_state
                  module: timeflux.nodes.events
                  class: States
                  params:
                    states:
                      - solid
                      - liquid
                      - gas
                    # initial is optional, defaults to first state listed.
                    #initial: solid
                    transitions:
                      melt:
                        from: solid
                        to: liquid
                      freeze:
                        # from is optional, defaults to all other states
                        # from: liquid, gas
                        to: solid
                      gas: # Specifying a gas transition explicitly overrides the default transition from any state to gas
                        from: liquid
                        to: gas
                      # These transition are present automatically and so do not need to be specified
                      #solid:
                      #  from: liquid, gas
                      #  to: solid
                      #liquid:
                      #  from: solid, gas
                      #  to: liquid

                - id: circadian_rhythym
                  module: timeflux.nodes.events
                  class: States
                  params:
                    states:
                      - sleeping
                      - sleepy
                      - awake
                    initial: awake
                    properties:
                      sleepy:
                        min_seconds: 600 # It takes at least 10 mins to wake up or fall asleep
                      awake:
                        auto_transition: sleepy
                        auto_transition_seconds: 64800 # After 18 hours we're sure to be sleepy, though nothing stops us becoming sleepy sooner

    """

    def __init__(self, states, transitions={}, properties={}, initial=None, event_label=None, reflexive_transitions=False):
        self._states = states
        self._transitions = transitions
        self._event_label = event_label
        self._properties = properties

        # For every state add a direct transition from any other state.
        for state in self._states:
            if state not in self._transitions:
                self._transitions[state] = {'to': state}

        for transition_name, transition in self._transitions.items():
            if not 'from' in transition:
                froms = self._states
                if not reflexive_transitions:
                    froms = copy.copy(self._states)
                    froms.remove(transition['to'])
                self._transitions[transition_name]['from'] = froms

        # If the initial state is not specified, use the first listed state.
        self._state = initial if initial is not None else next(iter(self._states))
        if not self._state in self._states:
            raise Exception("Initial state must be one of the provided states.")

        self._events_carried_over = None
        self._blocked_until_time = None
        self._last_transition_time = None
        self._auto_transition_time = None


        for state, state_properties in self._properties.items():
            if 'min_seconds' in self._properties[state]:
                self._properties[state]['min_seconds'] = pd.Timedelta(seconds=self._properties[state]['min_seconds'])
            if 'auto_transition_seconds' in self._properties[state]:
                self._properties[state]['auto_transition_seconds'] = pd.Timedelta(seconds=self._properties[state]['auto_transition_seconds'])
        for state in self._states:
            if not state in self._properties:
                self._properties[state] = {}

    def update(self):
        self.o.meta = self.i.meta
        self.o_transitions.meta = self.i.meta
        self._state_series = pd.Series(dtype='object')
        self._transition_series = pd.Series(dtype='object')

        if self.i.ready():
            self._end_time = self.i.data.index[-1]
            self._state_series = pd.Series(index=self.i.data.index, dtype="object")
            self._state_series.iloc[0] = self._state
            self._last_transition_time = self._last_transition_time if self._last_transition_time is not None else self.i.data.index[0]
            self._has_state_indices = True

        else: # We can switch states even without a source of time indices, the indices will come from events or auto-transitions.
            self._end_time = np.datetime64(datetime.datetime.utcnow())
            self._has_state_indices = False

        events = self._get_events()
        self._do_transitions(events)

        # States and transitions can be indexed differently so don't combine into a single dataframe.
        self.o.data = pd.DataFrame({'state': self._state_series})
        self.o_transitions.data = pd.DataFrame({'transition': self._transition_series})


    def _do_transitions(self, events):
        # Iterate over the events, and respond to them where appropriate.
        if events is not None:
            for index, event in events.iteritems():
                self._do_auto_transitions(index)
                if self._is_valid_transition(index, event):
                    self.logger.debug(f"Transition '{event}' triggered by event at {index}.")
                    self._transition(index, event)

        # Process any autotransitions that fall between the last event and the end of the iteration.
        self._do_auto_transitions(self._end_time)

        if self._has_state_indices:
            # Backfill the state since the last transition.
            self._state_series.loc[self._last_transition_time:self._end_time] = self._state


    def _do_auto_transitions(self, until):
        while self._auto_transition_time is not None and until > self._auto_transition_time:
            self.logger.debug(f"Transition '{self._auto_transition}' triggered by time at {self._auto_transition_time}.")
            self._transition(self._auto_transition_time, self._auto_transition)


    def _is_valid_transition(self, index, transition):
        if self._blocked_until_time is not None and self._blocked_until_time > index:
            return False
        if not self._state in self._transitions[transition].get('from'):
            return False
        return True


    def _transition(self, index, transition):
        if not transition in self._transitions:
            raise Exception("Invalid transition '{}'".format(transition))

        self._transition_series[index] = transition
        if self._has_state_indices:
            # We are evaluating the state at a series of predefined indices.
            # The indices we are specifying the state for come from i, while the events may come from i or i_events and
            # therefore may be indexed differently. Therefore we need to use slices on the state series.
            # Backfill the previous state up until the transition.
            # The state at the right edge (==index) will be overwritten on the next transition.
            self._state_series.loc[self._last_transition_time:index] = self._state
        else:
            # We are not evaluating the state at a series of predefined indices, we create an index only when we transition.
            self._state_series.loc[index] = self._transitions[transition]['to']

        self._state = self._transitions[transition]['to']
        self._last_transition_time = index

        if 'min_seconds' in self._properties[self._state]:
            self._blocked_until_time = index + self._properties[self._state]['min_seconds']

        # Cancel any existing auto transition and set a new one if appropriate.
        self._auto_transition_time = None
        self._auto_transition = None
        if 'auto_transition' in self._properties[self._state]:
            self._auto_transition_time = index + self._properties[self._state]['auto_transition_seconds']
            self._auto_transition = self._properties[self._state]['auto_transition']
            self.logger.debug(f"Auto transition '{self._auto_transition}' set for {self._auto_transition_time}.")

    def _get_events(self):
        # Get the events from the i_events port if that port exists, otherwise fall back to the main input port.
        events = None
        if 'i_events' in self.ports:
            if self.i_events.ready():
                events = self.i_events.data
        else:
            events = self.i.data

        if events is not None:
            # Identify the column to find events in.
            self._event_label = self._event_label if self._event_label is not None else events.columns[0]
            # Discard irrelevant events and columns.
            events = events[self._event_label]
            events = events.where(events.isin(self._transitions))
            events = events.dropna()

        # Handle events that occur out of scope. This can happen if i_events is indexed differently to i.
        if not (events is None and self._events_carried_over is None):
            # Either events or self._events_carried_over can be None, but it's fine to concat a None and a dataframe.
            events = pd.concat([self._events_carried_over, events])
            # Carry over events that are out of scope for this iteration
            self._events_carried_over = events[events.index > self._end_time]
        return events
