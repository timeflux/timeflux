"""timeflux.nodes.axis: helpers to manipulate axis"""

from timeflux.core.node import Node


class Rename(Node):
    def __init__(self, **kwargs):
        ''' Alter axes labels.

        Attributes:
            i (Port): Default data input, expects DataFrame.
            o (Port): Default output, provides DataFrame and meta.

        Args:
            kwargs: see arguments from
            [pandas.DataFrame.rename method](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rename.html)
        '''
        super().__init__()
        self._kwargs = kwargs

    def update(self):
        if self.i.ready():
            self.o = self.i
            self.o.data.rename(**self._kwargs, inplace=True)


class RenameColumns(Node):
    def __init__(self, names):
        """ Rename column labels from a list

        Attributes:
            i (Port): Default data input, expects DataFrame.
            o (Port): Default output, provides DataFrame and meta.

        Args:
            names (list): New column names
        """

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


class AddSuffix(Node):
    def __init__(self, suffix):
        """ Suffix labels with string suffix.

        Attributes:
            i (Port): Default data input, expects DataFrame.
            o (Port): Default output, provides DataFrame and meta.

        Args:
            suffix (string): The string to add after each column label
        """
        super().__init__()
        if not isinstance(suffix, str):
            raise ValueError('suffix should be a string')
        self._suffix = suffix

    def update(self):
        if not self.i.ready():
            return
        self.o.meta = self.i.meta
        self.o.data = self.i.data.add_suffix(self._suffix)
