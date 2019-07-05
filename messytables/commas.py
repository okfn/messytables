import re
import csv
import six
import logging

from messytables.buffered import BUFFER_SIZE
from messytables.text import analyze_stream
from messytables.core import RowSet, TableSet, Cell
from messytables.error import ReadError

DELIMITERS = ['\t', ',', ';', '|']
LINE_SEPARATOR = ['\r\n', '\r', '\n', '\0']

# Fix the maximum field size to something a little larger
csv.field_size_limit(256000)
log = logging.getLogger(__name__)


class CSVTableSet(TableSet):
    """A CSV table set.

    Since CSV is always just a single table, this is just a pass-through for
    the row set.
    """

    def __init__(self, fileobj, delimiter=None, quotechar=None, name=None,
                 encoding=None, window=1000, doublequote=True,
                 skipinitialspace=None, **kw):
        self._tables = [CSVRowSet(name or 'table', fileobj,
                                  delimiter=delimiter,
                                  quotechar=quotechar,
                                  encoding=encoding,
                                  window=window,
                                  doublequote=doublequote,
                                  skipinitialspace=skipinitialspace)]

    def make_tables(self):
        """Return the actual CSV table."""
        return self._tables


class TSVTableSet(CSVTableSet):
    """A TSV table set.

    This is a slightly specialised version of the CSVTableSet that will always
    generate a tab-based table parser.
    """

    def __init__(self, fileobj, quotechar=None, name=None,
                 encoding=None, window=1000, doublequote=True,
                 skipinitialspace=None, **kw):
        super(TSVTableSet, self).__init__(fileobj, delimiter='\t',
                                          quotechar=quotechar, name=name,
                                          encoding=encoding, window=window,
                                          doublequote=doublequote,
                                          skipinitialspace=skipinitialspace,
                                          **kw)


class CSVRowSet(RowSet):
    """A CSV row set is an iterator on a CSV file-like object.

    (which can potentially be infinetly large). When loading,
    a sample is read and cached so you can run analysis on the
    fragment.
    """

    def __init__(self, name, fileobj, delimiter=None, quotechar=None,
                 encoding=None, window=1000, doublequote=None,
                 skipinitialspace=None):
        self.name = name
        self.encoding, self.buf = analyze_stream(fileobj, encoding=encoding)
        self.fileobj = fileobj

        # For line breaking, use the (detected) encoding of the file:
        linesep = [t.encode(self.encoding) for t in LINE_SEPARATOR]
        linesep = b'(' + b'|'.join(linesep) + b')'
        self.linesep = re.compile(linesep)

        self._sample = []
        self.window = window

        try:
            sample = self.buf.decode(self.encoding)
            if six.PY2:
                sample = sample.encode('utf-8')
            self.dialect = csv.Sniffer().sniff(sample, delimiters=DELIMITERS)
        except csv.Error:
            self.dialect = csv.excel
        # override detected dialect with constructor values.
        self.dialect.delimiter = delimiter or str(self.dialect.delimiter)
        self.dialect.quotechar = quotechar or str(self.dialect.quotechar)
        if skipinitialspace is not None:
            self.dialect.skipinitialspace = skipinitialspace
        if doublequote is not None:
            self.dialect.doublequote = doublequote
        super(CSVRowSet, self).__init__()

    def get_lines(self, sample=False):
        for line in self._sample:
            yield line

        while True:
            if self.buf is None:
                break
            if sample and len(self._sample) >= self.window:
                break
            match = self.linesep.search(self.buf)
            if match is not None:
                line = self.buf[:match.end(0)]
                self.buf = self.buf[match.end(0):]
            else:
                buf = self.fileobj.read(BUFFER_SIZE)
                if len(buf):
                    self.buf += buf
                    continue
                else:
                    line, self.buf = self.buf, None

            line = line.decode(self.encoding)
            if six.PY2:
                line = line.encode('utf-8')

            if line in LINE_SEPARATOR or not len(line):
                continue

            if self.window >= len(self._sample):
                self._sample.append(line)
            yield line

    def raw(self, sample=False):
        try:
            for row in csv.reader(self.get_lines(sample=sample),
                                  dialect=self.dialect):
                if six.PY2:
                    row = [c.decode('utf-8') for c in row]
                yield [Cell(c) for c in row]
        except csv.Error as err:
            if 'new-line character' not in repr(err):
                raise ReadError('Error reading CSV: %r', err)
