"""A set of Port helpers."""

import json
import pandas as pd
from timeflux.helpers.clock import now


def make_event(label, data={}):
    """ Create an event DataFrame

    Args:
        label (str): The event label.
        data (dict): The optional data dictionary.

    Returns:
        Dataframe

    """
    return pd.DataFrame(
        [[label, json.dumps(data)]], index=[now()], columns=["label", "data"]
    )


def match_events(port, label):
    """ Find the given label in an event DataFrame

    Args:
        port (Port): The event port.
        label (str): The string to look for in the label column.

    Returns:
        DataFrame: The list of matched events, or `None` if there is no match.

    """
    matches = None
    if port.ready():
        matches = port.data[port.data["label"] == label]
        if matches.empty:
            matches = None
    return matches


def get_meta(port, keys, default=None):
    """ Find a deep value in a port's meta

    Args:
        port (Port): The event port.
        keys (tuple|str): The hiearchical list of keys.
        default: The default value if not found.

    Returns:
        The value, or `default` if not found.

    """
    return traverse(port.meta, keys, default)


def traverse(dictionary, keys, default=None):
    """ Find a deep value in a dictionary

    Args:
        dictionary (dict): The event port.
        keys (tuple|str): The hiearchical list of keys.
        default: The default value if not found.

    Returns:
        The value, or `default` if not found.

    """
    if keys is None:
        return default
    if type(keys) == str:
        keys = (keys,)
    for key in keys:
        try:
            dictionary = dictionary[key]
        except KeyError:
            return default
    return dictionary
