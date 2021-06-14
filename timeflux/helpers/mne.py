"""MNE helpers"""

import pandas as pd
import numpy as np
import xarray as xr
import logging
from timeflux.core.exceptions import WorkerInterrupt

try:
    import mne
except ModuleNotFoundError:
    raise SystemExit(
        "MNE is not installed. Optional dependencies can be installed with: 'pip install timeflux[opt]'."
    )

logger = logging.getLogger()


def _context_to_id(context, context_key, event_id):
    if context_key is None:
        return context
    else:
        return event_id.get(context.get(context_key))


def xarray_to_mne(
    data, meta, context_key, event_id, reporting="warn", ch_types="eeg", **kwargs
):
    """Convert DataArray and meta into mne Epochs object

    Args:
        data (DataArray): Array of dimensions ('epoch', 'time', 'space')
        meta (dict): Dictionary with keys 'epochs_context', 'rate', 'epochs_onset'
        context_key (str|None): key to select the context label.
        If the context is a string, `context_key` should be set to ``None``.
        event_id (dict): Associates context label to an event_id that should be an int. (eg. dict(auditory=1, visual=3))
        reporting ('warn'|'error'| None): How this function handles epochs with invalid context:
            - 'error' will raise a TimefluxException
            - 'warn' will print a warning with :py:func:`warnings.warn` and skip the corrupted epochs
            - ``None`` will skip the corrupted epochs
        ch_types (list|str): Channel type to
    Returns:
        epochs (mne.Epochs): mne object with the converted data.
    """
    if isinstance(ch_types, str):
        ch_types = [ch_types] * len(data.space)

    if isinstance(data, xr.DataArray):
        pass
    elif isinstance(data, xr.Dataset):
        # extract data
        data = data.data
    else:
        raise ValueError(
            f"data should be of type DataArray or Dataset, received {data.type} instead. "
        )
    _dims = data.coords.dims
    if "target" in _dims:
        np_data = data.transpose("target", "space", "time").values
    elif "epoch" in _dims:
        np_data = data.transpose("epoch", "space", "time").values
    else:
        raise ValueError(
            f"Data should have either `target` or `epoch` in its coordinates. Found {_dims}"
        )
    # create events objects are essentially numpy arrays with three columns:
    # event_sample | previous_event_id | event_id

    events = np.array(
        [
            [onset.value, 0, _context_to_id(context, context_key, event_id)]
            for (context, onset) in zip(meta["epochs_context"], meta["epochs_onset"])
        ]
    )  # List of three arbitrary events
    events_mask = np.isnan(events.astype(float))[:, 2]
    if events_mask.any():
        if reporting == "error":
            raise WorkerInterrupt(
                f"Found {events_mask.sum()} epochs with corrupted context. "
            )
        else:  # reporting is either None or warn
            # be cool, skip those evens
            events = events[~events_mask, :]
            np_data = np_data[~events_mask, :, :]
            if reporting == "warn":
                logger.warning(
                    f"Found {events_mask.sum()} epochs with corrupted context. "
                    f"Skipping them. "
                )
    # Fill the second column with previous event ids.
    events[0, 1] = events[0, 2]
    events[1:, 1] = events[0:-1, 2]
    # set the info
    rate = meta["rate"]
    info = mne.create_info(
        ch_names=list(data.space.values), sfreq=rate, ch_types=ch_types
    )
    # construct the mne object
    epochs = mne.EpochsArray(
        np_data,
        info=info,
        events=events.astype(int),
        event_id=event_id,
        tmin=data.time.values[0] / np.timedelta64(1, "s"),
        verbose=False,
        **kwargs,
    )
    return epochs


def mne_to_xarray(epochs, context_key, event_id, output="dataarray"):
    """Convert mne Epochs object into DataArray along with meta.

    Args:
        epochs (mne.Epochs): mne object with the converted data.
        context_key (str|None): key to select the context label.
        If the context is a string, `context_key` should be set to ``None``.
        event_id (dict): Associates context label to an event_id that should be an int.
                        (eg. dict(auditory=1, visual=3))
        output (str): type of the expected output (DataArray or Dataset)

    Returns:
        data (DataArray|Dataset): Array of dimensions ('epoch', 'time', 'space')
        meta (dict): Dictionary with keys 'epochs_context', 'rate', 'epochs_onset'

    """
    reversed_event_id = {value: key for (key, value) in event_id.items()}
    np_data = epochs._data
    ch_names = epochs.ch_names
    epochs_onset = [pd.Timestamp(event_sample) for event_sample in epochs.events[:, 0]]
    epochs_context = [
        {context_key: reversed_event_id[_id]} for _id in epochs.events[:, 2]
    ]
    meta = dict(
        epochs_onset=epochs_onset,
        epochs_context=epochs_context,
        rate=epochs.info["sfreq"],
    )
    n_epochs = len(epochs)
    times = pd.TimedeltaIndex(data=epochs.times, unit="s")
    data = xr.DataArray(
        np_data,
        dims=("epoch", "space", "time"),
        coords=(np.arange(n_epochs), ch_names, times),
    ).transpose("epoch", "time", "space")
    if output == "dataarray":
        return data, meta
    else:  # output == 'dataset'
        data = xr.Dataset(
            {
                "data": data,
                "target": [reversed_event_id[_id] for _id in epochs.events[:, 2]],
            }
        )
        return data, meta
