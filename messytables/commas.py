import csv

from six import text_type, PY2

from messytables.buffered import seekable_stream, BUFFER_SIZE
from messytables.text import UTF8Recoder, to_unicode_or_bust
from messytables.core import RowSet, TableSet, Cell
from messytables.error import ReadError

DELIMITERS = ['\t', ',', ';', '|']

# Fix the maximum field size to something a little larger
csv.field_size_limit(256000)


class CSVTableSet(TableSet):
    """ A CSV table set. Since CSV is always just a single table,
    this is just a pass-through for the row set. """

    def __init__(self, fileobj, delimiter=None, quotechar=None, name=None,
                 encoding=None, window=None, doublequote=True,
                 lineterminator=None, skipinitialspace=None, **kw):
        self.fileobj = seekable_stream(fileobj)
        self.name = name or 'table'
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.encoding = encoding
        self.window = window
        self.doublequote = doublequote
        self.lineterminator = lineterminator
        self.skipinitialspace = skipinitialspace

    def make_tables(self):
        """Return the actual CSV table."""
        return [CSVRowSet(self.name, self.fileobj,
                          delimiter=self.delimiter,
                          quotechar=self.quotechar,
                          encoding=self.encoding,
                          window=self.window,
                          doublequote=self.doublequote,
                          lineterminator=self.lineterminator,
                          skipinitialspace=self.skipinitialspace)]


class CSVRowSet(RowSet):
    """ A CSV row set is an iterator on a CSV file-like object
    (which can potentially be infinetly large). When loading,
    a sample is read and cached so you can run analysis on the
    fragment. """

    def __init__(self, name, fileobj, delimiter=None, quotechar=None,
                 encoding='utf-8', window=None, doublequote=True,
                 lineterminator=None, skipinitialspace=None):
        self.name = name
        self.fh = seekable_stream(fileobj)
        self.fileobj = UTF8Recoder(self.fh, encoding)

        def fake_ilines(fobj):
            for row in fobj:
                yield row.decode('utf-8')
        self.lines = fake_ilines(self.fileobj)
        self._sample = []
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.window = window or 1000
        self.doublequote = doublequote
        self.lineterminator = lineterminator
        self.skipinitialspace = skipinitialspace
        try:
            for i in range(self.window):
                self._sample.append(next(self.lines))
        except StopIteration:
            pass
        super(CSVRowSet, self).__init__()

    @property
    def _dialect(self):
        delim = '\n'  # NATIVE
        sample = delim.join(self._sample)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=DELIMITERS)
            dialect.delimiter = self.delimiter or str(dialect.delimiter)
            dialect.quotechar = self.quotechar or str(dialect.quotechar)
            dialect.lineterminator = self.lineterminator or delim
            if self.skipinitialspace is not None:
                dialect.skipinitialspace = self.skipinitialspace
            if self.lineterminator is not None:
                dialect.lineterminator = self.lineterminator
            dialect.doublequote = True
            return dialect
        except csv.Error:
            return csv.excel

    def raw(self, sample=False):
        def rows():
            for line in self._sample:
                if PY2:
                    yield line.encode('utf-8')
                else:
                    yield line
            if not sample:
                for line in self.lines:
                    if PY2:
                        yield line.encode('utf-8')
                    else:
                        yield line

        try:
            for row in csv.reader(rows(), dialect=self._dialect):
                yield [Cell(to_unicode_or_bust(c)) for c in row]
        except csv.Error as err:
            if u'newline inside string' in text_type(err) and sample:
                pass
            elif u'line contains NULL byte' in text_type(err):
                pass
            else:
                raise ReadError('Error reading CSV: %r', err)
