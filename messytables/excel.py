from datetime import datetime
import xlrd

from messytables.core import RowSet, TableSet, Cell
from messytables.types import StringType, IntegerType, \
        DateType, FloatType


XLS_TYPES = {
    1: StringType(),
    # NB: Excel does not distinguish floats from integers so we use floats
    # We could try actual type detection between floats and ints later
    # or use the excel format string info - see
    # https://groups.google.com/forum/?fromgroups=#!topic/python-excel/cAQ1ndsCVxk
    2: FloatType(),
    3: DateType(None),
    # this is actually boolean but we do not have a boolean type yet
    4: IntegerType()
}


class XLSTableSet(TableSet):
    """An excel workbook wrapper object.
    """

    def __init__(self, fileobj=None, filename=None, window=None, encoding=None):
        '''Initilize the tableset.

        :param encoding: passed on to xlrd.open_workbook function as encoding_override
        '''
        from xlrd.biffh import XLRDError

        self.window = window
        try:
            if filename:
                self.workbook = xlrd.open_workbook(filename,
                                                   encoding_override=encoding)
            elif fileobj:
                self.workbook = xlrd.open_workbook(file_contents=fileobj.read(),
                                                   encoding_override=encoding)
            else:
                raise Exception('You must provide one of filename of fileobj')
        except XLRDError:
            raise XLRDError("Unsupported format, or corrupt file")

    @property
    def tables(self):
        """ Return the sheets in the workbook. """
        return [XLSRowSet(name, self.workbook.sheet_by_name(name), self.window) \
                for name in self.workbook.sheet_names()]


class XLSRowSet(RowSet):
    """ Excel support for a single sheet in the excel workbook. Unlike
    the CSV row set this is not a streaming operation. """

    def __init__(self, name, sheet, window=None):
        self.name = name
        self.sheet = sheet
        self.window = window or 1000
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
                        raise ValueError('Invalid date at "%s":%d,%d' % (self.sheet.name, j + 1, i + 1))
                    year, month, day, hour, minute, second = \
                            xlrd.xldate_as_tuple(value, self.sheet.book.datemode)
                    value = datetime(year, month, day, hour,
                            minute, second)
                row.append(Cell(value, type=type))
            yield row

