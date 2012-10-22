from tempfile import mkstemp
from datetime import datetime
from shutil import copyfileobj
from itertools import islice
import xlrd

from messytables.core import RowSet, TableSet, Cell
from messytables.types import StringType, IntegerType, \
        DateType

XLS_TYPES = {
    # TODO: extend with float etc.
    1: StringType(),
    2: IntegerType(),
    3: DateType(None)
    }

class XLSTableSet(TableSet):
    """ An excel workbook wrapper object. As the underlying
    library is based on reading from a file name (as opposed to
    a file object), a local, temporary copy is created and
    passed into the library. This has significant performance
    implication for large excel sheets. """

    def __init__(self, filename):
        self.workbook = xlrd.open_workbook(filename)

    @classmethod
    def from_fileobj(cls, fileobj):
        """ Create a local copy of the object and attempt
        to open it with xlrd. """
        fd, name = mkstemp(suffix='xls')
        copyfileobj(fileobj, open(name, 'wb'))
        return cls(name)

    @property
    def tables(self):
        """ Return the sheets in the workbook. """
        return [XLSRowSet(name, self.workbook.sheet_by_name(name)) \
                for name in self.workbook.sheet_names()]


class XLSRowSet(RowSet):
    """ Excel support for a single sheet in the excel workbook. Unlike
    the CSV row set this is not a streaming operation. """

    def __init__(self, name, sheet, window=1000):
        self.name = name
        self.sheet = sheet
        self.window = window
        super(XLSRowSet, self).__init__(typed=True)

    def raw(self, sample=False):
        """ Iterate over all rows in this sheet. Types are automatically
        converted according to the excel data types specified, including
        conversion of excel dates, which are notoriously buggy. """
        num_rows = self.sheet.nrows
        for i in xrange(min(self.window, num_rows) if sample else num_rows):
            row = []
            for j, cell in enumerate(self.sheet.row(i)):
                value = cell.value
                type = XLS_TYPES.get(cell.ctype, StringType())
                if type == DateType(None):
                    if value == 0:
                        raise ValueError('Invalid date at "%s":%d,%d' % (self.sheet.name, j+1, i+1))
                    year, month, day, hour, minute, second = \
                            xlrd.xldate_as_tuple(value, self.sheet.book.datemode)
                    value = datetime(year, month, day, hour,
                            minute, second)
                row.append(Cell(value, type=type))
            yield row

