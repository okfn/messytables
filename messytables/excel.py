import sys
from datetime import datetime, time
import xlrd
from xlrd.biffh import XLRDError

from messytables.core import RowSet, TableSet, Cell, CoreProperties
from messytables.types import (StringType, IntegerType,
                               DateType, FloatType)
from messytables.error import ReadError
from messytables.compat23 import PY2

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

    def __init__(self, fileobj=None, filename=None, window=None,
                 encoding=None, with_formatting_info=True, **kw):
        '''Initialize the tableset.

        :param encoding: passed on to xlrd.open_workbook function
            as encoding_override
        :param with_formatting_info: passed to xlrd to get font details of cells
        '''
        def get_workbook():
            try:
                return xlrd.open_workbook(
                    filename=filename,
                    file_contents=read_obj,
                    encoding_override=encoding,
                    formatting_info=with_formatting_info)
            except XLRDError as e:
                _, value, traceback = sys.exc_info()
                if PY2:
                   raise ReadError("Can't read Excel file: %r" % value, traceback)
                else:
                   raise ReadError("Can't read Excel file: %r" % value).with_traceback(traceback)
        '''Initilize the tableset.

        :param encoding: passed on to xlrd.open_workbook function
            as encoding_override
        :param with_formatting_info: whether xlrd should provide details
            of the cells contents (e.g. colour, borders, etc.
            Not sure what the behaviour of properties is with this turned off.
            Turning this on apparently may have memory implications in xlrd.

        The convoluted "try it with with_formatting_info, then try it without" is
        necessary because xlrd doesn't currently support getting this information
        from XLSX files. Workarounds include converting the XLSX document in LibreOffice.
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
        for rownum in range(min(self.window, num_rows) if sample else num_rows):
            row = []
            for colnum, cell in enumerate(self.sheet.row(rownum)):
                try:
                    row.append(XLSCell.from_xlrdcell(cell, self.sheet, colnum, rownum))
                except InvalidDateError:
                    raise ValueError("Invalid date at '%s':%d,%d" % (
                        self.sheet.name, colnum+1, rownum+1))
            yield row

class XLSCell(Cell):
    @staticmethod
    def from_xlrdcell(xlrd_cell, sheet, col, row):
        value = xlrd_cell.value
        cell_type = XLS_TYPES.get(xlrd_cell.ctype, StringType())
        if cell_type == DateType(None):
            if value == 0:
                raise InvalidDateError
            year, month, day, hour, minute, second = \
                xlrd.xldate_as_tuple(value, sheet.book.datemode)
            if (year, month, day) == (0, 0, 0):
                value = time(hour, minute, second)
            else:
                value = datetime(year, month, day, hour, minute, second)
        messy_cell = XLSCell(value, type=cell_type)
        messy_cell.sheet = sheet
        messy_cell.xlrd_cell = xlrd_cell
        messy_cell.xlrd_pos = (row, col)  # necessary for properties, note not (x,y)
        return messy_cell

    @property
    def topleft(self):
        return self.properties.topleft

    @property
    def properties(self):
        return XLSProperties(self)

class XLSProperties(CoreProperties):
    KEYS = ['bold', 'size', 'italic', 'font_name', 'strikeout', 'underline',
            'font_colour', 'background_colour', 'any_border', 'all_border',
            'richtext', 'blank', 'a_date', 'formatting_string']
    def __init__(self, cell):
        self.cell = cell
        self.merged = {}

    @property
    def xf(self):
        return self.cell.sheet.book.xf_list[self.cell.xlrd_cell.xf_index]

    @property
    def font(self):
        return self.cell.sheet.book.font_list[self.xf.font_index]

    @property
    def formatting(self):
        return self.cell.sheet.book.format_map[self.xf.format_key]

    @property
    def rich(self):
        """returns a tuple of character position, font number which starts at that position
        https://secure.simplistix.co.uk/svn/xlrd/trunk/xlrd/doc/xlrd.html?p=4966#sheet.Sheet.rich_text_runlist_map-attribute"""
        return self.cell.sheet.rich_text_runlist_map.get(self.cell.xlrd_pos, None)

    def raw_span(self, always=False):
        """return the bounding box of the cells it's part of.
         https://secure.simplistix.co.uk/svn/xlrd/trunk/xlrd/doc/xlrd.html?p=4966#sheet.Sheet.merged_cells-attribute"""
        row, col = self.cell.xlrd_pos
        for box in self.cell.sheet.merged_cells:
            rlo, rhi, clo, chi = box
            # note the high indexes are NOT inclusive!
            rhi = rhi - 1
            chi = chi - 1
            if row >= rlo and row <= rhi and col >= clo and col <= chi:
                return rlo, rhi, clo, chi
        if always:
            return (row, row, col, col)
        else:
            return None

    @property
    def topleft(self):
        span = self.raw_span()
        if span is None:
            return True  # is a single cell
        else:
            rlo, _, clo, _ = span
            return (rlo, clo) == self.cell.xlrd_pos

    def get_formatting_string(self):
        return self.formatting.format_str

    def get_a_date(self):
        return self.formatting.type == 1

    def get_richtext(self):  # TODO - get_rich_fragments
        return bool(self.rich)

    def get_bold(self):
        return self.font.weight > 500

    def get_size(self):
        """in pixels"""
        return self.font.height / 20.0

    def get_italic(self):
        return bool(self.font.italic)

    def get_font_name(self):
        return self.font.name

    def get_strikeout(self):
        return bool(self.font.struck_out)

    def get_underline(self):
        return self.font.underline_type > 0

    def get_font_colour(self):
        # TODO
        return self.font.color_index ## more lookup required

    def get_blank(self):
        """Note that cells might not exist at all.
           Behaviour for spanned cells might be complicated: hence this function"""
        return self.cell.value == ''

    def get_background_colour(self):
        return self.xf.background.background_color_index ## more lookup required

    def get_any_border(self):
        b = self.xf.border
        return b.top_line_style > 0 or b.bottom_line_style > 0 or \
               b.left_line_style > 0 or b.right_line_style > 0

    def get_all_border(self):
        b = self.xf.border
        return b.top_line_style > 0 and b.bottom_line_style > 0 and \
               b.left_line_style > 0 and b.right_line_style > 0

