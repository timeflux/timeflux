import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin, ClassifierMixin


class Transpose(BaseEstimator, TransformerMixin):
    def __init__(self, axes):
        self._axes = axes

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        return np.transpose(X, self._axes)

    def fit_transform(self, X, y=None):
        return self.fit(X).transform(X)


class Expand(BaseEstimator, TransformerMixin):
    def __init__(self, axis=0, dimensions=3):
        self._axis = axis
        self._dimensions = dimensions

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = np.asarray(X)
        if len(X.shape) < self._dimensions:
            return np.expand_dims(X, axis=self._axis)
        else:
            return X

    def fit_transform(self, X, y=None):
        return self.fit(X).transform(X)


class Reduce(BaseEstimator, TransformerMixin):
    def __init__(self, axis=0):
        self._axis = axis

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = np.asarray(X)
        if X.ndim < 3:
            return X
        return np.squeeze(X, axis=self._axis)

    def fit_transform(self, X, y=None):
        return self.fit(X).transform(X)
