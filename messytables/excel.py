from tempfile import mkstemp
from datetime import datetime
from shutil import copyfileobj
from itertools import islice
import xlrd

from messytables.core import RowSet, TableSet, Cell
from messytables.types import StringType, IntegerType, \
        DateType

XLS_TYPES = {
    1: StringType(), 
    2: IntegerType(),
    3: DateType(None)
    }

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


class XLSRowSet(RowSet):
    """ Excel support for a single sheet in the excel workbook. Unlike
    the CSV row set this is not a streaming operation. """

    def __init__(self, name, sheet, window=1000):
        self.name = name
        self.sheet = sheet
        self.window = window
        super(XLSRowSet, self).__init__()

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

