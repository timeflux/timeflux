"""Enumerate groups in a HFD5 file."""

import sys
import pandas as pd


def info(fname):
    store = pd.HDFStore(fname, "r")
    for key in store.keys():
        print(key)
    store.close()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.exit()
    fname = sys.argv[1]
    info(fname)
