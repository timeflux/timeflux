"""Enumerate LSL streams."""

from pylsl import resolve_streams


def enumerate():
    streams = resolve_streams()
    for stream in streams:
        print(
            f"name: {stream.name()}, type: {stream.type()}, source_id: {stream.source_id()}"
        )


if __name__ == "__main__":
    enumerate()
