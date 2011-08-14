from datetime import datetime
from collections import defaultdict

from messytables.util import skip_n
from messytables.types import type_guess
from messytables.types import StringType, IntegerType, FloatType, \
        DecimalType, DateType

from messytables.core import Cell, TableSet, RowSet

class CSVTableSet(TableSet):
    """ A CSV table set. Since CSV is always just a single table,
    this is only a pass-through class. """

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


from ilines import ilines
from itertools import chain
import csv
class CSVRowSet(RowSet):

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

    typed = False

    @property
    def sample_lines(self):
        for line in self._sample:
            yield line

    def rows(self):
        for line in chain(self.sample_lines, self.lines):
            yield line

    @property
    def dialect(self):
        sample = '\n'.join(self.sample_lines)
        return csv.Sniffer().sniff(sample)

    @property
    def sample(self):
        for row in csv.reader(self.sample_lines, dialect=self.dialect):
            yield self.apply_types([Cell(c) for c in row])

    def raw(self):
        for row in csv.reader(self.rows(), dialect=self.dialect):
            yield self.apply_types([Cell(c) for c in row])


from tempfile import mkstemp
from shutil import copyfileobj
import xlrd
class XLSTableSet(TableSet):

    def __init__(self, filename):
        self.workbook = xlrd.open_workbook(filename)

    @classmethod
    def from_fileobj(cls, fileobj):
        # xlrd reads from a file name, no fileobj support
        fd, name = mkstemp(suffix='xls')
        copyfileobj(fileobj, open(name, 'wb'))
        return cls(name)

    @property
    def tables(self):
        return [XLSRowSet(name, self.workbook.sheet_by_name(name)) \
                for name in self.workbook.sheet_names()]

XLS_TYPES = {
    1: StringType(), 
    2: IntegerType(),
    3: DateType(None)
    }

from itertools import islice
class XLSRowSet(RowSet):

    def __init__(self, name, sheet, window=1000):
        self.name = name
        self.sheet = sheet
        self.window = window

    typed = True

    @property
    def sample(self):
        return islice(self, self.window)

    def raw(self):
        """ Iterate over all rows in this sheet. Types are automatically
        converted according to the excel data types specified, including 
        conversion of excel dates, which are notoriously buggy. """
        for i in xrange(self.sheet.nrows):
            row = []
            for cell in self.sheet.row(i):
                value = cell.value
                type = XLS_TYPES.get(cell.ctype, StringType())
                if type == DateType(None):
                    year, month, day, hour, minute, second = \
                            xlrd.xldate_as_tuple(value, self.sheet.book.datemode)
                    value = datetime(year, month, day, hour, 
                            minute, second)
                row.append(Cell(value, type=type))
            yield row



def column_count_modal(rows):
    """ Return the modal value of columns in the row_set's 
    sample. """
    counts = defaultdict(int)
    for row in rows:
        length = len([c for c in row if not c.empty])
        counts[length] += 1
    if not len(counts):
        return 0
    return max(counts.items(), key=lambda (k,v): v)[0]


def headers_guess(rows, tolerance=1):
    """ Guess the offset and names of the headers of the row set.
    This will attempt to locate the first row within ``tolerance``
    of the mode of the number of rows in the row set sample.
    """
    rows = list(rows)
    modal = column_count_modal(rows)
    for i, row in enumerate(rows):
        length = len([c for c in row if not c.empty])
        if length >= modal - tolerance:
            # TODO: use type guessing to check that this row has
            # strings and does not conform to the type schema of 
            # the table.
            return i,  [c.value for c in row]
    return 0, []




