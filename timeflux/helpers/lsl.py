"""Enumerate LSL streams."""

import logging

logger = logging.getLogger(__name__)

try:
    from pylsl import resolve_streams
except ModuleNotFoundError:
    logger.exception('pylsl is not installed, you will encounter NameErrors '
                     'unless you install pylsl')


def enumerate_streams():
    streams = resolve_streams()
    for stream in streams:
        print(f'name: {stream.name()}, type: {stream.type()}, source_id: {stream.source_id()}')


if __name__ == '__main__':
    enumerate_streams()
