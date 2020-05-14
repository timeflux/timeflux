"""Enumerate groups in a HFD5 file."""

import sys
import tables


def info(fname):
    f = tables.open_file(fname, "r")
    print(f)
    f.close()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.exit()
    fname = sys.argv[1]
    info(fname)
