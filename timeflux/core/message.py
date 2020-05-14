"""timeflux.core.message: serialize and unserialize dataframes"""

import pickle
import pandas as pd

# import pyarrow as pa


def pickle_serialize(message):
    topic = message[0]
    data = message[1]
    meta = message[2]
    return [
        topic,
        pickle.dumps(data, pickle.HIGHEST_PROTOCOL),
        pickle.dumps(meta, pickle.HIGHEST_PROTOCOL),
    ]


def pickle_deserialize(message):
    topic = message[0].decode("utf-8")
    data = message[1]
    meta = message[2]
    return [topic, pickle.loads(data), pickle.loads(meta)]


def msgpack_serialize(message):
    # TODO: handle meta and cases where data is None
    topic = message[0]
    data = message[1]
    return [topic, data.to_msgpack()]


def msgpack_deserialize(message):
    # TODO: handle meta and cases where data is None
    topic = message[0].decode("utf-8")
    data = message[1]
    return [topic, pd.read_msgpack(data)]


# def arrow_serialize(message):
#     topic = message[0].decode('utf-8')
#     df = message[1]
#     return [topic, pa.serialize(df).to_buffer()]

# def arrow_deserialize(message):
#     topic = message[0]
#     data = message[1]
#     return [topic, pa.deserialize(data)]
