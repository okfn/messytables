from ilines import ilines
from itertools import chain
import csv
import codecs
import chardet

from messytables.core import RowSet, TableSet, Cell


class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f, 'ignore')
        
        # The reader only skips a BOM if the encoding isn't explicit about its
        # endianness (i.e. if encoding is UTF-16 a BOM is handled properly
        # and taken out, but if encoding is UTF-16LE a BOM is ignored).
        # However, if chardet sees a BOM it returns an encoding with the
        # endianness explicit, which results in the codecs stream leaving the
        # BOM in the stream. This is ridiculously dumb. For UTF-{16,32}{LE,BE}
        # encodings, check for a BOM and remove it if it's there.
        if encoding in ("UTF-16LE", "UTF-16BE", "UTF-32LE", "UTF-32BE"):
        	bom = getattr(codecs, "BOM_UTF" + encoding[4:6] + "_" + encoding[-2:], None)
        	if bom:
        		# Try to read the BOM, which is a byte sequence, from the underlying
        		# stream. If all characters match, then go on. Otherwise when a character
        		# doesn't match, seek the stream back to the beginning and go on.
        		for c in bom:
        			if f.read(1) != c:
        				f.seek(0)
        				break

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

    def __init__(self, fileobj, delimiter=None, name=None, encoding=None):
        self.fileobj = fileobj
        self.name = name or 'table'
        self.delimiter = delimiter or ','
        if not encoding:
            buf = fileobj.read(100)
            results = chardet.detect(buf)
            self.encoding = results['encoding']
            fileobj.seek(0)

    @classmethod
    def from_fileobj(cls, fileobj, delimiter=',', name=None):
        return cls(fileobj, delimiter=delimiter, name=name)

    @property
    def tables(self):
        """ Return the actual CSV table. """
        return [CSVRowSet(self.name, self.fileobj,
                          delimiter=self.delimiter,
                          encoding=self.encoding)]


class CSVRowSet(RowSet):
    """ A CSV row set is an iterator on a CSV file-like object
    (which can potentially be infinetly large). When loading,
    a sample is read and cached so you can run analysis on the
    fragment. """

    def __init__(self, name, fileobj, delimiter=None,
                 encoding='utf-8', window=1000):
        self.name = name
        self.fileobj = UTF8Recoder(fileobj, encoding)
        self.lines = ilines(self.fileobj)
        self._sample = []
        self.delimiter = delimiter or ','
        try:
            for i in xrange(window):
                self._sample.append(self.lines.next())
        except StopIteration:
            pass
        super(CSVRowSet, self).__init__()

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

    def raw(self, sample=False):
        def rows():
            for line in self._sample:
                yield line
            if not sample:
                for line in self.lines:
                    yield line
        try:
            for row in csv.reader(rows(), delimiter=self.delimiter, dialect=self._dialect):
                yield [Cell(to_unicode_or_bust(c)) for c in row]
        except csv.Error, err:
            if 'newline inside string' in unicode(err) and sample:
                pass
            elif 'line contains NULL byte' in unicode(err):
                pass
            else:
                raise
