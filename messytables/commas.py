from ilines import ilines
from itertools import chain
import csv
import codecs

from messytables.core import RowSet, TableSet, Cell


class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f, 'ignore')

    def __iter__(self):
        return self

    def next(self):
        line = self.reader.readline()
        if not line or line == '\0':
            raise StopIteration
        result = line.encode("utf-8")
        return result


def to_unicode_or_bust(obj, encoding='utf-8'):
    if isinstance(obj, basestring):
        if not isinstance(obj, unicode):
            obj = unicode(obj, encoding)
    return obj


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

    def __init__(self, name, fileobj, encoding='utf-8', window=1000):
        self.name = name
        self.fileobj = UTF8Recoder(fileobj, encoding)
        self.lines = ilines(self.fileobj)
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
        sample = delim.join(self._sample)
        try:
            dialect = csv.Sniffer().sniff(sample,
                delimiters=['\t', ',', ';'])
            dialect.lineterminator = delim
            dialect.doublequote = True
            return dialect
        except csv.Error:
            return csv.excel

    @property
    def sample(self):
        def rows():
            for line in self._sample_lines:
                yield line
        try:
            for row in csv.reader(rows(), dialect=self._dialect):
                yield [Cell(to_unicode_or_bust(c)) for c in row]
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
                yield line
        try:
            for row in csv.reader(rows(), dialect=self._dialect):
                yield [Cell(to_unicode_or_bust(c)) for c in row]
        except csv.Error, err:
            if 'line contains NULL byte' in unicode(err):
                pass
            else:
                raise
