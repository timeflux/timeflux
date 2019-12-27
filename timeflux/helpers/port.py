"""A set of Port helpers."""

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
        matches = port.data[port.data['label'] == label]
        if matches.empty:
            matches = None
    return matches


def get_meta(port, keys, default=None):
    """ Find a deep value in a port's meta

    Args:
        port (Port): The event port.
        keys (tuple): The hiearchical list of keys.
        default: The default value if not found.

    Returns:
        The value, or `default` if not found.

    """
    return traverse(port.meta, keys, default)


def traverse(dictionary, keys, default=None):
    """ Find a deep value in a dictionary

    Args:
        dictionary (dict): The event port.
        keys (tuple): The hiearchical list of keys.
        default: The default value if not found.

    Returns:
        The value, or `default` if not found.

    """
    for key in keys:
        try:
            dictionary = dictionary[key]
        except KeyError:
            return default
    return dictionary
