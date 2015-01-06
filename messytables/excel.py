import sys
from datetime import datetime
import xlrd
from xlrd.biffh import XLRDError

from messytables.core import RowSet, TableSet, Cell, CoreProperties
from messytables.types import (StringType, IntegerType,
                               DateType, FloatType)
from messytables.error import ReadError

class InvalidDateError(Exception):
    pass

XLS_TYPES = {
    1: StringType(),
    # NB: Excel does not distinguish floats from integers so we use floats
    # We could try actual type detection between floats and ints later
    # or use the excel format string info - see
    # https://groups.google.com/forum/?fromgroups=#!topic/
    #  python-excel/cAQ1ndsCVxk
    2: FloatType(),
    3: DateType(None),
    # this is actually boolean but we do not have a boolean type yet
    4: IntegerType()
}

class XLSTableSet(TableSet):
    """An excel workbook wrapper object.
    """

    def __init__(self, fileobj=None, filename=None,
                 window=None, encoding=None, with_formatting_info=True):
        def get_workbook():
            print filename, len(read_obj)
            try:
                return xlrd.open_workbook(
                    filename=filename,
                    file_contents=read_obj,
                    encoding_override=encoding,
                    formatting_info=with_formatting_info)
            except XLRDError as e:
                _, value, traceback = sys.exc_info()
                raise ReadError, "Can't read Excel file: %r" % value, traceback
        '''Initilize the tableset.

        :param encoding: passed on to xlrd.open_workbook function
            as encoding_override
        '''
        self.window = window

        if not filename and not fileobj:
            raise Exception('You must provide one of filename or fileobj')

        if fileobj:
            read_obj = fileobj.read()
        else:
            read_obj = None

        try:
            self.workbook = get_workbook()
        except NotImplementedError as e:
            if not with_formatting_info:
                raise
            else:
                with_formatting_info=False
                self.workbook = get_workbook()


    def make_tables(self):
        """ Return the sheets in the workbook. """
        return [XLSRowSet(name, self.workbook.sheet_by_name(name), self.window)
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
                try:
                    row.append(XLSCell.from_xlrdcell(cell, self.sheet))
                except InvalidDateError:
                    raise ValueError("Invalid date at '%s':%d,%d" % (
                        self.sheet.name, j+1, i+1))
            yield row

class XLSCell(Cell):
    @staticmethod
    def from_xlrdcell(xlrd_cell, sheet):
        value = xlrd_cell.value
        cell_type = XLS_TYPES.get(xlrd_cell.ctype, StringType())
        if cell_type == DateType(None):
            if value == 0:
                raise InvalidDateError
            year, month, day, hour, minute, second = \
                xlrd.xldate_as_tuple(value, sheet.book.datemode)
            value = datetime(year, month, day, hour,
                             minute, second)
        messy_cell = XLSCell(value, type=cell_type)
        messy_cell.sheet = sheet
        messy_cell.xlrd_cell = xlrd_cell
        return messy_cell

    @property
    def topleft(self):
        # TODO
        return True

    @property
    def properties(self):
        return XLSProperties(self)

class XLSProperties(CoreProperties):
    KEYS = ['bold']
    def __init__(self, cell):
        self.cell = cell

    def get_bold(self):
        return True


