import csv
import codecs
import chardet

from messytables.core import RowSet, TableSet, Cell
import messytables
from messytables.compat23 import unicode_string, byte_string, native_string, PY2


class UTF8Recoder:
    """
    Iterator that reads an encoded stream and re-encodes the input to UTF-8
    """

    # maps between chardet encoding and codecs bom keys
    BOM_MAPPING = {
        'utf-16le': 'BOM_UTF16_LE',
        'utf-16be': 'BOM_UTF16_BE',
        'utf-32le': 'BOM_UTF32_LE',
        'utf-32be': 'BOM_UTF32_BE',
        'utf-8': 'BOM_UTF8',
        'utf-8-sig': 'BOM_UTF8',

    }

    def __init__(self, f, encoding):
        sample = f.read(2000)
        if not encoding:
            results = chardet.detect(sample)
            encoding = results['encoding']
            if not encoding:
                # Don't break, just try and load the data with
                # a semi-sane encoding
                encoding = 'utf-8'
        f.seek(0)
        self.reader = codecs.getreader(encoding)(f, 'ignore')

        # The reader only skips a BOM if the encoding isn't explicit about its
        # endianness (i.e. if encoding is UTF-16 a BOM is handled properly
        # and taken out, but if encoding is UTF-16LE a BOM is ignored).
        # However, if chardet sees a BOM it returns an encoding with the
        # endianness explicit, which results in the codecs stream leaving the
        # BOM in the stream. This is ridiculously dumb. For UTF-{16,32}{LE,BE}
        # encodings, check for a BOM and remove it if it's there.
        if encoding.lower() in self.BOM_MAPPING:
            bom = getattr(codecs, self.BOM_MAPPING[encoding.lower()], None)
            if bom:
                # Try to read the BOM, which is a byte sequence, from
                # the underlying stream. If all characters match, then
                # go on. Otherwise when a character doesn't match, seek
                # the stream back to the beginning and go on.
                for c in bom:
                    if f.read(1) != c:
                        f.seek(0)
                        break

    def __iter__(self):
        return self

    def __next__(self):
        line = self.reader.readline()
        if not line or line == '\0':
            raise StopIteration
        result = line.encode("utf-8")
        return result

    next = __next__


def to_unicode_or_bust(obj, encoding='utf-8'):
    if isinstance(obj, byte_string):
        obj = unicode_string(obj, encoding)
    return obj


class CSVTableSet(TableSet):
    """ A CSV table set. Since CSV is always just a single table,
    this is just a pass-through for the row set. """

    def __init__(self, fileobj, delimiter=None, quotechar=None, name=None,
                 encoding=None, window=None, doublequote=None,
                 lineterminator=None, skipinitialspace=None, **kw):
        self.fileobj = messytables.seekable_stream(fileobj)
        self.name = name or 'table'
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.encoding = encoding
        self.window = window
        self.doublequote = doublequote
        self.lineterminator = lineterminator
        self.skipinitialspace = skipinitialspace

    def make_tables(self):
        """ Return the actual CSV table. """
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
                 encoding='utf-8', window=None, doublequote=None,
                 lineterminator=None, skipinitialspace=None):
        self.name = name
        seekable_fileobj = messytables.seekable_stream(fileobj)
        self.fileobj = UTF8Recoder(seekable_fileobj, encoding)

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
            dialect = csv.Sniffer().sniff(sample,
                delimiters=['\t', ',', ';', '|'])  # NATIVE
            dialect.delimiter = native_string(dialect.delimiter)
            dialect.quotechar = native_string(dialect.quotechar)
            dialect.lineterminator = delim
            dialect.doublequote = True
            return dialect
        except csv.Error:
            return csv.excel

    @property
    def _overrides(self):
        # some variables in the dialect can be overridden
        d = {}
        if self.delimiter:
            d['delimiter'] = self.delimiter
        if self.quotechar:
            d['quotechar'] = self.quotechar
        if self.doublequote:
            d['doublequote'] = self.doublequote
        if self.lineterminator:
            d['lineterminator'] = self.lineterminator
        if self.skipinitialspace is not None:
            d['skipinitialspace'] = self.skipinitialspace
        return d

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

        # Fix the maximum field size to something a little larger
        csv.field_size_limit(256000)

        try:
            for row in csv.reader(rows(),
                                  dialect=self._dialect, **self._overrides):
                yield [Cell(to_unicode_or_bust(c)) for c in row]
        except csv.Error as err:
            if u'newline inside string' in unicode_string(err) and sample:
                pass
            elif u'line contains NULL byte' in unicode_string(err):
                pass
            else:
                raise messytables.ReadError('Error reading CSV: %r', err)
