import os
import logging
import pandas as pd

try:
    import pyedflib
except ModuleNotFoundError:
    raise SystemExit(
        "PyEDFlib is not installed. Optional dependencies can be installed with: 'pip install timeflux[opt]'."
    )

logger = logging.getLogger()


def convert(src, dst=None, data_key="eeg", events_key="events"):
    if not dst:
        dst = os.path.splitext(src)[0] + ".bdf"
    try:
        store = pd.HDFStore(src, "r")
    except:
        return _error(f"Could not read from file: {src}")
    try:
        data = store.select(data_key)
    except:
        return _error("Data key not found.")
    try:
        rate = store.get_node(data_key)._v_attrs["meta"]["rate"]
    except:
        logger.warning("Data rate not set.")
        rate = None
    start = data.index[0]
    channels = list(data.columns)
    signals = data.values.T
    dmin = -8388608
    dmax = 8388607
    pmin = signals.min()
    pmax = signals.max()
    try:
        n_channels = len(channels)
        file_type = 3  # BDF+
        bdf = pyedflib.EdfWriter(dst, n_channels=n_channels, file_type=file_type)
    except:
        return _error(f"Could not write to file: {src}")
    headers = []
    for channel in channels:
        headers.append(
            {
                "label": channel,
                "dimension": "uV",
                "sample_rate": rate,
                "physical_min": pmin,
                "physical_max": pmax,
                "digital_min": dmin,
                "digital_max": dmax,
                "transducer": "",
                "prefilter": "",
            }
        )
    bdf.setSignalHeaders(headers)
    bdf.setStartdatetime(start)
    bdf.writeSamples(signals)
    try:
        events = store.select(events_key)
    except:
        logger.warning("Events key not found.")
        events = False
    if events is not False:
        for event in events.itertuples():
            onset = (event.Index - start).total_seconds()
            duration = 0
            description = event.label
            bdf.writeAnnotation(onset, duration, description)  # meta is lost
    bdf.close()
    store.close()


def _error(message):
    logger.error(message)
    return False


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()
    parser.add_argument("input", help="input path")
    parser.add_argument("-o", "--output", default=None, help="output path")
    parser.add_argument("-d", "--data", default="eeg", help="data key (default: 'eeg')")
    parser.add_argument(
        "-e", "--events", default="events", help="events key (default: 'events')"
    )
    args = parser.parse_args()
    convert(args.input, args.output, args.data, args.events)
