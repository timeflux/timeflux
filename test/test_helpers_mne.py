import numpy as np
import pandas as pd
import xarray as xr
from timeflux.helpers.mne import mne_to_xarray, xarray_to_mne

rate = 10.0  # Sampling rate
# Generate epochs data, ie. random data in shape (n_epochs, n_channels, n_times)
times = pd.TimedeltaIndex(np.arange(-.2, .8, 1 / rate), unit='s')
n_epochs = 3
n_channels = 5
n_times = len(times)

channels = [f'ch{k}' for k in range(n_channels)]
# Generate data and meta as it were from output port of node eEpochsToDataArray
original_data = xr.DataArray(np.random.rand(n_epochs, n_times, n_channels),
                             dims=('epoch', 'time', 'space'),
                             coords=(np.arange(n_epochs), times, channels))
original_meta = {'epochs_context': [{'target': 'foo'}] + [{'target': 'bar'}] * (n_epochs - 1),
                 'rate': rate,
                 'epochs_onset': list(pd.date_range(
                     start='2018-01-01',
                     periods=n_epochs,
                     freq=pd.DateOffset(seconds=1)))}
# context_key is the key to select the label in the context
context_key = 'target'
event_id = {'foo': 0, 'bar': 1}


def test_mne_dataarray_conversions():
    epochs = xarray_to_mne(original_data, original_meta, context_key, event_id)
    after_conversion_data, after_conversion_meta = mne_to_xarray(epochs, context_key, event_id)
    # assert that the passage in mne keeps the data unchanged
    xr.testing.assert_equal(after_conversion_data, original_data)
    assert original_meta == after_conversion_meta
