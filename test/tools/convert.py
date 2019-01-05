import sys
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime


infile = 'data.orig.hdf5'
outfile = 'data.hdf5'
keys = ['/nexus/signal/nexus_signal_raw', '/unity/events/unity_events']
#keys = ['/nexus/signal/nexus_signal_raw']

try:
    store = pd.HDFStore(infile, mode='r')
except IOError as e:
    print(f'Could not open file {infile}')
    sys.exit(1)

try:
    os.remove(outfile)
except:
    pass

for key in keys:
    print(f'Loading {key}')
    try:
        df = pd.read_hdf(store, key)
    except:
        print('Key not found')
        continue
    for column in df:
        if type(df[column].iloc[0]) == dict:
            print(f'Converting [{column}] column to JSON')
            df[column] = df[column].map(json.dumps)
    if type(df.index[0]) == np.float64:
        print('Converting indices')
        df.index = df.index.map(datetime.utcfromtimestamp)
        df.index = pd.to_datetime(df.index, unit='us', utc=True)
    elif type(df.index[0]) == pd.Timestamp:
        print('Timestamp index found')
    else:
        print('Invalid index type')
        continue
    print(f'Saving to {outfile}')
    df.to_hdf(outfile, key, mode='a', format='table')
    #df.to_csv('data.csv', mode='w')

