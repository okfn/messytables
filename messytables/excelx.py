from tempfile import mkstemp
from datetime import datetime
from shutil import copyfileobj
from itertools import islice
from openpyxl.reader.excel import load_workbook

from messytables.core import RowSet, TableSet, Cell
from messytables.types import StringType, IntegerType, \
        DateType

class XLSXTableSet(TableSet):
    """ An excel workbook wrapper object. As the underlying
    library is based on reading from a file name (as opposed to
    a file object), a local, temporary copy is created and 
    passed into the library. This has significant performance 
    implication for large excel sheets. """

    def __init__(self, filename):
        self.workbook = load_workbook(filename=filename)

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
        return [XLSXRowSet(sheet) for sheet in self.workbook.worksheets]


class XLSXRowSet(RowSet):
    """ Excel support for a single sheet in the excel workbook. Unlike
    the CSV row set this is not a streaming operation. """

    def __init__(self, sheet, window=1000):
        self.name = sheet.title
        self.sheet = sheet
        self.window = window
        super(XLSXRowSet, self).__init__(typed=True)

    def raw(self, sample=False):
        """ Iterate over all rows in this sheet. Types are automatically
        converted according to the excel data types specified, including 
        conversion of excel dates, which are notoriously buggy. """
        num_rows = self.sheet.get_highest_row()
        for i in xrange(min(self.window, num_rows) if sample else num_rows):
            row = []
            for cell in self.sheet.rows[i]:
                value = cell.value
                if cell.is_date():
                    type = DateType(None)
                elif cell.data_type == 'n':
                    type = IntegerType()
                else:
                    type = StringType()
                row.append(Cell(value, type=type))
            yield row
