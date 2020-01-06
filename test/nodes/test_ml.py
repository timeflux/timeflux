import pytest
import logging
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin, ClassifierMixin
from timeflux.core.exceptions import ValidationError, WorkerInterrupt
from timeflux.helpers.testing import DummyData
from timeflux.helpers.clock import now, time_range
from timeflux.helpers.port import make_event
from timeflux.nodes.ml import Pipeline


class DummyClassifierUnsupervised(BaseEstimator, ClassifierMixin):

    def fit(self, X, y=None):
        return self

    def predict(self, X):
        return np.random.rand(X.shape[0])

    def fit_predict(self, X, y=None):
        return self.fit(X).predict(X)


class DummyTransformer(BaseEstimator, TransformerMixin):

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return X * 2

    def fit_transform(self, X, y=None):
        return self.fit(X).transform(X)


class Flattener(BaseEstimator, TransformerMixin):

    def __init__(self, order='C'):
        self.order = order

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return np.asarray(X).flatten(self.order)

    def fit_transform(self, X, y=None):
        return self.fit(X).transform(X)


class Vectorizer(BaseEstimator, TransformerMixin):

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return np.asarray(X).reshape(len(X), -1)

    def fit_transform(self, X, y=None):
        return self.fit(X).transform(X)


class Shaper(BaseEstimator, TransformerMixin):

    def __init__(self, shape=(1,)):
        self.shape = shape

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return np.asarray(X).reshape(self.shape)

    def fit_transform(self, X, y=None):
        return self.fit(X).transform(X)


dummy_classifier = [{'module': 'sklearn.dummy', 'class': 'DummyClassifier', 'args': {'strategy': 'most_frequent'}}]
dummy_regressor = [{'module': 'sklearn.dummy', 'class': 'DummyRegressor'}]
dummy_transformer = [{'module': 'test_ml', 'class': 'DummyTransformer'}]


def test_passthrough():
    node = Pipeline(steps=dummy_classifier, passthrough=True)
    streamer = DummyData()
    node.i_training.data = streamer.next()
    node.i_training_0.data = streamer.next()
    node.i_events.data = make_event('foobar')
    node.i.data = streamer.next()
    node.i_0.data = streamer.next()
    node.i_1.data = streamer.next()
    node.i.meta = {'foobar': 42}
    node.update()
    assert len(list(node.iterate('o*'))) == 3
    assert node.o.data.equals(node.i.data)
    assert node.o_0.data.equals(node.i_0.data)
    assert node.o_0.data.equals(node.i_0.data)
    assert node.o.meta == node.i.meta

def test_2D_training(random):
    node = Pipeline(steps=dummy_classifier)
    node.i_training.data = DummyData().next()
    node.update()
    assert node._dimensions == 2

def test_3D_training(random):
    node = Pipeline(steps=dummy_classifier)
    node.i_training_0.data = DummyData().next()
    node.update()
    assert node._dimensions == 3

def test_2D_no_training(random):
    node = Pipeline(steps=dummy_transformer, mode='fit_transform')
    node.i.data = DummyData().next()
    node.update()
    assert node._dimensions == 2

def test_3D_no_training(random):
    node = Pipeline(steps=dummy_classifier, mode='fit_predict')
    node.i_0.data = DummyData().next()
    node.update()
    assert node._dimensions == 3

def test_accumulation_boundaries():
    node = Pipeline(steps=dummy_classifier)
    events = [
        ['accumulation_starts', ''],
        ['accumulation_stops', ''],
        ['accumulation_starts', '']
    ]
    times = pd.date_range(start='2018-01-01', periods=3, freq='1s')
    node.i_events.set(events, times, ['label', 'data'])
    node.update()
    assert node._accumulation_start == np.datetime64('2018-01-01T00:00:00')
    assert node._accumulation_stop == np.datetime64('2018-01-01T00:00:01')

def test_idle_buffer_2D(random):
    start = now() - pd.Timedelta('10s')
    stream = DummyData(start_date=start, rate=1, jitter=0)
    node = Pipeline(steps=dummy_classifier, buffer_size='5s')
    node.i_training.data = stream.next(10)
    node.update()
    assert len(node._X_train_indices) == 4
    node.i_training.data = stream.next(10)
    node.update()
    assert len(node._X_train_indices) == 14
    assert len(node._X_train_indices) == len(node._X_train)

def test_accumulate_invalid_shape_2D(caplog):
    stream = DummyData(start_date=now(), num_cols=5)
    node = Pipeline(steps=dummy_classifier)
    node.i_training.data = DummyData(start_date=now(), num_cols=5).next(10)
    node.update()
    node.i_training.data = DummyData(start_date=now(), num_cols=6).next(10)
    node.update()
    assert caplog.record_tuples[0][2] == 'Invalid shape'
    assert len(node._X_train_indices) == len(node._X_train)

def test_accumulate_start_2D(random):
    node = Pipeline(steps=dummy_classifier, buffer_size='5s')
    start = now()
    node.i_events.set([['accumulation_starts', '']], [start], ['label', 'data'])
    stream = DummyData(start_date=start, rate=1, jitter=0)
    node.i_training.data = stream.next(100)
    node.update()
    assert len(node._X_train) == 100

def test_accumulate_start_stop_2D(random):
    node = Pipeline(steps=dummy_classifier, buffer_size='5s')
    start = now()
    events = [
        ['accumulation_starts', ''],
        ['accumulation_stops', '']
    ]
    times = pd.date_range(start=start, periods=2, freq='10s')
    node.i_events.set(events, times, ['label', 'data'])
    stream = DummyData(start_date=start, rate=1, jitter=0)
    node.i_training.data = stream.next(100)
    node.update()
    assert len(node._X_train) == 10

def test_idle_buffer_3D(random):
    node = Pipeline(steps=dummy_classifier, buffer_size='5s', meta_label=None)
    start_0 = now() - pd.Timedelta('10s')
    start_1 = now()
    start_2 = now() + pd.Timedelta('10s')
    node.i_training_0.data = DummyData(start_date=start_0).next(10)
    node.i_training_1.data = DummyData(start_date=start_1).next(10)
    node.i_training_2.data = DummyData(start_date=start_2).next(10)
    node.update()
    assert len(node._X_train_indices) == 2
    assert len(node._X_train_indices) == len(node._X_train)
    assert node._X_train.shape == (2, 10, 5)

def test_accumulate_invalid_shape_3D(caplog):
    node = Pipeline(steps=dummy_classifier, meta_label=None)
    start_0 = now()
    start_1 = now() + pd.Timedelta('1s')
    start_2 = now() + pd.Timedelta('2s')
    node.i_training_0.data = DummyData(start_date=start_0, num_cols=5).next(10)
    node.i_training_1.data = DummyData(start_date=start_1, num_cols=6).next(10)
    node.i_training_2.data = DummyData(start_date=start_2, num_cols=5).next(20)
    node.update()
    assert caplog.record_tuples[0][2] == caplog.record_tuples[1][2] == 'Invalid shape'

def test_accumulate_y_train(caplog):
    node = Pipeline(steps=dummy_classifier)
    stream = DummyData(start_date=now())
    node.i_training_0.data = stream.next()
    node.i_training_1.data = stream.next()
    node.i_training_2.data = stream.next()
    node.i_training_0.meta = { 'epoch': { 'context': { 'target': True }}}
    node.i_training_1.meta = {}
    node.i_training_2.meta = { 'epoch': { 'context': { 'target': False }}}
    node.update()
    assert node._y_train.tolist() == [True, False]
    assert caplog.record_tuples[0][2] =='Invalid label'

def test_trim_2D(random):
    node = Pipeline(steps=dummy_classifier)
    data = DummyData(rate=1).next(20)
    node._X_train = data.values
    node._X_train_indices = np.array(data.index.values, dtype=np.datetime64)
    node._accumulation_start = np.datetime64('2018-01-01T00:00:05')
    node._accumulation_stop = np.datetime64('2018-01-01T00:00:15')
    node._accumulate()
    assert len(node._X_train_indices) == 10
    assert len(node._X_train) == 10

def test_trim_3D(random):
    node = Pipeline(steps=dummy_classifier)
    node.i_training_0.data = DummyData(start_date='2018-01-01T00:00:00').next()
    node.i_training_1.data = DummyData(start_date='2018-01-01T00:00:10').next()
    node.i_training_2.data = DummyData(start_date='2018-01-01T00:00:20').next()
    node.i_training_3.data = DummyData(start_date='2018-01-01T00:00:30').next()
    node.i_training_0.meta = { 'epoch': { 'context': { 'target': 1 }}}
    node.i_training_1.meta = { 'epoch': { 'context': { 'target': 2 }}}
    node.i_training_2.meta = { 'epoch': { 'context': { 'target': 3 }}}
    node.i_training_3.meta = { 'epoch': { 'context': { 'target': 4 }}}
    node._accumulation_start = np.datetime64('2017-12-31T00:00:00')
    node._accumulation_stop = np.datetime64('2018-01-01T00:01:00')
    node._status = 1
    node.update()
    node._dimensions = 0 # Bypass accumulation
    node._accumulation_start = np.datetime64('2018-01-01T00:00:05')
    node._accumulation_stop = np.datetime64('2018-01-01T00:00:25')
    node._accumulate()
    assert len(node._X_train_indices) == 2
    assert len(node._X_train) == 2
    assert len(node._y_train) == 2
    assert node._y_train.tolist() == [2, 3]

def test_index_dtype():
    rows = [[0, 0], [0, 0]]
    indices = np.array(['2001-01-01', '2001-01-02'], dtype='datetime64')
    assert indices.dtype == 'datetime64[D]'
    data = pd.DataFrame(rows, indices)
    assert data.index.values.dtype == 'datetime64[ns]'
    node = Pipeline(steps=dummy_classifier)
    node.i_training.data = data
    node.update()

def test_make_pipeline_invalid_steps():
    with pytest.raises(ValidationError) as e:
        Pipeline(steps=None)
    assert str(e.value) == "Validation error for param `steps`: None is not of type 'array'"

def test_make_pipeline_invalid_module():
    steps = [{'module': 'foo', 'class': 'bar'}]
    with pytest.raises(ValidationError) as e:
        Pipeline(steps=steps)
    assert str(e.value) == "Validation error for param `steps`: could not import 'foo'"

def test_make_pipeline_invalid_class():
    steps = [{'module': 'sklearn.dummy', 'class': 'Foo'}]
    with pytest.raises(ValidationError) as e:
        Pipeline(steps=steps)
    assert str(e.value) == "Validation error for param `steps`: could not find class 'Foo'"

def test_make_pipeline_invalid_args():
    steps = [{'module': 'sklearn.dummy', 'class': 'DummyClassifier', 'args': {'foo': 'bar'}}]
    with pytest.raises(ValidationError) as e:
        Pipeline(steps=steps)
    assert str(e.value) == "Validation error for param `steps`: could not instantiate class 'DummyClassifier' with the given params"

def test_make_pipeline_success():
    node = Pipeline(steps=dummy_classifier)
    assert str(type(node._pipeline)) == "<class 'sklearn.pipeline.Pipeline'>"

def test_fit_success(caplog):
    caplog.set_level(logging.DEBUG)
    node = Pipeline(steps=dummy_classifier)
    node._status = -1 # bypass accumulation
    assert hasattr(node._pipeline[0], 'n_classes_') == False
    node._X_train = np.array([-1, 1, 1, 1])
    node._y_train = np.array([0, 1, 1, 1])
    node.i_events.data = make_event('training_starts')
    while node._status != 3:
        node.update()
    assert node._pipeline[0].n_classes_ == 2
    assert caplog.record_tuples[0][2].startswith('Model fitted in')

def test_fit_error(caplog):
    steps = [{
        'module': 'sklearn.dummy',
        'class': 'DummyClassifier',
        'args': {
            'strategy': 'foobar'
        }
    }]
    node = Pipeline(steps=steps)
    node.i_events.data = make_event('training_starts')
    with pytest.raises(WorkerInterrupt):
        while node._status != 3:
            node.update()
    assert caplog.record_tuples[0][2].startswith('An error occured while fitting')

def test_fit_interrupt():
    node = Pipeline(steps=dummy_classifier)
    node.terminate()

def test_receive_2D():
    node = Pipeline(steps=dummy_transformer, fit=False, mode='transform')
    node.i.data = DummyData().next()
    node.update()
    assert node._X.shape == (10, 5)
    assert node._dimensions == 2

def test_receive_2D_invalid_shape(caplog):
    node = Pipeline(steps=dummy_transformer, fit=True, mode='transform')
    node.i_training.data = DummyData(start_date=now(), num_cols=5).next()
    node.update()
    assert node._shape == 5
    node._status = 3
    node.i.data = DummyData(start_date=now(), num_cols=3).next()
    node.update()
    assert caplog.record_tuples[0][2] == 'Invalid shape'
    assert node._X == None

def test_receive_3D_supervised():
    node = Pipeline(steps=dummy_transformer, fit=False, mode='transform', meta_label='target')
    node.i_0.data = DummyData().next()
    node.i_0.meta = {'target': True}
    node.update()
    assert node._X[0].shape == (10, 5)
    assert node._y == [True]
    assert node._dimensions == 3

def test_receive_3D_unsupervised():
    node = Pipeline(steps=dummy_transformer, fit=False, mode='transform', meta_label=None)
    node.i_0.data = DummyData().next()
    node.update()
    assert node._X[0].shape == (10, 5)
    assert node._y == None
    assert node._dimensions == 3

def test_receive_3D_invalid_shape(caplog):
    node = Pipeline(steps=dummy_transformer, fit=True, mode='transform', meta_label=None)
    node.i_training_0.data = DummyData(start_date=now()).next(5)
    node.update()
    assert node._shape == (5, 5)
    node._status = 3
    node.i_0.data = DummyData(start_date=now()).next(3)
    node.update()
    assert caplog.record_tuples[0][2] == 'Invalid shape'
    assert node._X == None

def test_receive_3D_invalid_label(caplog):
    node = Pipeline(steps=dummy_classifier, mode='fit_predict')
    node.i_0.data = DummyData().next()
    node.update()
    assert caplog.record_tuples[0][2] == 'Invalid label'
    assert node._X == None

def test_predict():
    # classifier = [
    #     {'module': 'test_node_ml', 'class': 'Flattener'},
    #     {'module': 'sklearn.dummy', 'class': 'DummyClassifier', 'args': {'strategy': 'most_frequent'}}
    # ]
    node = Pipeline(steps=dummy_classifier, mode='predict', meta_label='target')
    node.i_training_0.set([-1], [now()], meta={ 'target': 0 })
    node.i_training_1.set([1], [now()], meta={ 'target': 1 })
    node.i_training_2.set([1], [now()], meta={ 'target': 1 })
    node.i_training_3.set([1], [now()], meta={ 'target': 1 })
    node.i_events.data = make_event('training_starts')
    while node._status != 3:
        node.update()
    node.i_0.set([-1], [now()])
    node.i_1.set([1], [now()])
    node.i_2.set([1], [now()])
    node.i_3.set([1], [now()])
    node.update()
    assert list(node._out) == [1, 1, 1, 1]

def test_transform():
    node = Pipeline(steps=dummy_transformer, fit=False, mode='transform', meta_label=None)
    node.i.data = DummyData().next()
    node.update()
    expected = node.i.data.values * 2
    assert np.array_equal(expected, node._out)

def test_reindex_no_resample():
    node = Pipeline(steps=dummy_classifier)
    node.resample = False
    times = pd.date_range(start='2018-01-01', periods=10, freq='1s').values
    data = np.arange(50).reshape(10, 5)
    df = node._reindex(data, times)
    assert np.array_equal(df.index.values, times)

def test_reindex_resample_infer_exception():
    node = Pipeline(steps=dummy_classifier)
    node.resample = True
    node.resample_rate = None
    times = np.array([
        '2018-01-01T00:00:00.000000000',
        '2018-01-01T00:00:02.250000000'
        ], dtype=np.datetime64)
    data = np.arange(15).reshape(3, 5)
    with pytest.raises(ValueError):
        node._reindex(data, times)

def test_reindex_resample_right_infer():
    node = Pipeline(steps=dummy_classifier)
    node.resample = True
    node.resample_direction = 'right'
    node.resample_rate = None
    times = pd.date_range(start='2018-01-01', periods=10, freq='1s').values
    data = np.arange(25).reshape(5, 5)
    df = node._reindex(data, times)
    expected = np.array([
        '2018-01-01T00:00:00',
        '2018-01-01T00:00:01',
        '2018-01-01T00:00:02',
        '2018-01-01T00:00:03',
        '2018-01-01T00:00:04'
        ], dtype=np.datetime64)
    assert np.array_equal(df.index.values, expected)

def test_reindex_resample_left_infer():
    node = Pipeline(steps=dummy_classifier)
    node.resample = True
    node.resample_direction = 'left'
    node.resample_rate = None
    times = pd.date_range(start='2018-01-01', periods=10, freq='1s').values
    data = np.arange(25).reshape(5, 5)
    df = node._reindex(data, times)
    expected = np.array([
        '2018-01-01T00:00:05',
        '2018-01-01T00:00:06',
        '2018-01-01T00:00:07',
        '2018-01-01T00:00:08',
        '2018-01-01T00:00:09'
        ], dtype=np.datetime64)
    assert np.array_equal(df.index.values, expected)

def test_reindex_resample_both_infer():
    node = Pipeline(steps=dummy_classifier)
    node.resample = True
    node.resample_direction = 'both'
    node.resample_rate = None
    times = pd.date_range(start='2018-01-01', periods=10, freq='1s').values
    data = np.arange(25).reshape(5, 5)
    df = node._reindex(data, times)
    expected = np.array([
        '2018-01-01T00:00:02',
        '2018-01-01T00:00:03',
        '2018-01-01T00:00:04',
        '2018-01-01T00:00:05',
        '2018-01-01T00:00:06'
        ], dtype=np.datetime64)
    assert np.array_equal(df.index.values, expected)

def test_reindex_resample_right_given():
    node = Pipeline(steps=dummy_classifier)
    node.resample = True
    node.resample_direction = 'right'
    node.resample_rate = 0.5
    times = pd.date_range(start='2018-01-01', periods=10, freq='1s').values
    data = np.arange(25).reshape(5, 5)
    df = node._reindex(data, times)
    expected = np.array([
        '2018-01-01T00:00:00',
        '2018-01-01T00:00:02',
        '2018-01-01T00:00:04',
        '2018-01-01T00:00:06',
        '2018-01-01T00:00:08'
        ], dtype=np.datetime64)
    assert np.array_equal(df.index.values, expected)

def test_reindex_upsample_both_infer():
    node = Pipeline(steps=dummy_classifier)
    node.resample = True
    node.resample_direction = 'both'
    node.resample_rate = None
    times = pd.date_range(start='2018-01-01', periods=3, freq='1s').values
    data = np.arange(25).reshape(5, 5)
    df = node._reindex(data, times)
    expected = np.array([
        '2017-12-31T23:59:59',
        '2018-01-01T00:00:00',
        '2018-01-01T00:00:01',
        '2018-01-01T00:00:02',
        '2018-01-01T00:00:03'
        ], dtype=np.datetime64)
    assert np.array_equal(df.index.values, expected)

def test_predict_2D_output(random):
    classifier = [{'module': 'test_ml', 'class': 'DummyClassifierUnsupervised'}]
    node = Pipeline(steps=classifier, mode='fit_predict', meta_label=None)
    stream = DummyData(start_date=now())
    node.i.data = stream.next(5)
    node.update()
    assert len(node.o_events.data) == 5

def test_predict_3D_output():
    node = Pipeline(steps=dummy_classifier, mode='predict', meta_label='target')
    stream = DummyData(start_date=now())
    node.i_training_0.data = stream.next(5)
    node.i_training_1.data = stream.next(5)
    node.i_training_0.meta = { 'target': 0 }
    node.i_training_1.meta = { 'target': 1 }
    node.i_events.data = make_event('training_starts')
    while node._status != 3:
        node.update()
    node.i_0.data = stream.next(5)
    node.i_1.data = stream.next(5)
    node.update()
    assert len(node.o_events.data) == 2

def test_transform_2D_output(random):
    node = Pipeline(steps=dummy_transformer, mode='fit_transform')
    node.i.data = DummyData(start_date=now()).next()
    node.update()
    assert np.array_equal(node.i.data.index.values, node.o.data.index.values)

def test_transform_3D_output(random):
    pipeline = [
        {'module': 'test_ml', 'class': 'Vectorizer'},
        {'module': 'test_ml', 'class': 'DummyTransformer'},
        {'module': 'test_ml', 'class': 'Shaper', 'args': { 'shape': (2, -1, 5) }}
    ]
    node = Pipeline(steps=pipeline, mode='fit_transform', meta_label=None)
    stream = DummyData(start_date=now())
    node.i_0.data = stream.next()
    node.i_1.data = stream.next()
    node.update()
    assert len(list(node.iterate('o_*'))) == 2
    assert np.array_equal(node.i_0.data.index.values, node.o_0.data.index.values)

def test_reset():
    pass
