"""timeflux.nodes.pandas_helpers: Apply simple pandas methods on chunks inplace """

from timeflux.core.node import Node


class Fillna(Node):
    def __init__(self, **kwargs):
        ''' Fill NA/NaN values using the specified method
        Attributes:
            i (Port): Default input, expects DataFrame.
            o (Port): Default output, provides DataFrame and meta.
        '''
        super().__init__()
        self._kwargs = kwargs
        self._kwargs['inplace'] = True

    def update(self):
        if self.i.ready():
            self.o = self.i
            self.o.data.fillna(**self._kwargs)


class Rename(Node):
    """ Rename columns with a list
    """

    def __init__(self, names):
        super().__init__()
        if not isinstance(names, list):
            raise ValueError('names should be a list')
        self.names = names

    def update(self):
        if not self.i.ready():
            return
        data = self.i.data
        if data.shape[1] != len(self.names):
            self.logger.warning('Received unexpected shape! Expected %d, received %d columns',
                                len(self.names), data.shape[1])
            return
        data.columns = self.names
        self.o.set(data, data.index)


class Dropna(Node):
    def __init__(self, **kwargs):
        ''' Remove missing values.

        :param Keyword Arguments:
            * *axis*: Determine if rows or columns which contain missing values are removed. {O|"index"; 1|"columns"}
            * *how*: {"any", "all"} Determine if row or column is removed from DataFrame,
            * *thres*: Require that many non-NA values.
            * *subset*: Labels along other axis to consider.
        '''
        self._kwargs = kwargs
        self._kwargs['inplace'] = True

    def update(self):
        if not self.i.ready:
            return
        # copy the data
        self.o = self.i
        # at this point, we are sure we have data
        self.o.data.dropna(**self._kwargs)
