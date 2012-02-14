from ilines import ilines
from itertools import chain
import csv

from messytables.core import RowSet, TableSet, Cell

class CSVTableSet(TableSet):
    """ A CSV table set. Since CSV is always just a single table,
    this is just a pass-through for the row set. """

    def __init__(self, fileobj, name=None):
        self.fileobj = fileobj
        self.name = name or 'table'

    @classmethod
    def from_fileobj(cls, fileobj, name=None):
        return cls(fileobj, name=name)

    @property
    def tables(self):
        """ Return the actual CSV table. """
        return [CSVRowSet(self.name, self.fileobj)]

class CSVRowSet(RowSet):
    """ A CSV row set is an iterator on a CSV file-like object
    (which can potentially be infinetly large). When loading, 
    a sample is read and cached so you can run analysis on the
    fragment. """

    def __init__(self, name, fileobj, window=1000):
        self.name = name
        self.fileobj = fileobj
        self.lines = ilines(fileobj)
        self._sample = []
        try:
            for i in xrange(window):
                self._sample.append(self.lines.next())
        except StopIteration:
            pass
        super(CSVRowSet, self).__init__()

    @property
    def _sample_lines(self):
        for line in self._sample:
            yield line

    @property
    def _dialect(self):
        delim = '\n'
        sample = delim.join(self._sample).encode('utf-8')
        try:
            dialect = csv.Sniffer().sniff(sample, 
                delimiters=['\t',',',';'])
            dialect.lineterminator = delim
            return dialect
        except csv.Error:
            return csv.excel

    @property
    def sample(self):
        def rows():
            for line in self._sample_lines:
                yield line.encode('utf-8')
        try:
            for row in csv.reader(rows(), dialect=self._dialect):
                yield [Cell(c) for c in row]
        except csv.Error, err:
            if 'newline inside string' in unicode(err):
                pass
            elif 'line contains NULL byte' in unicode(err):
                pass
            else:
                raise

    def raw(self, sample=False):
        def rows():
            if sample:
                generator = self._sample_lines
            else:
                generator = chain(self._sample_lines, self.lines)
            for line in generator:
                yield line.encode('utf-8')
        try:
            for row in csv.reader(rows(), dialect=self._dialect):
                yield [Cell(c) for c in row]
        except csv.Error, err:
            if 'line contains NULL byte' in unicode(err):
                pass
            else:
                raise
