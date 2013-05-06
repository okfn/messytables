from shutil import copyfileobj
from openpyxl.reader.excel import load_workbook
import cStringIO

from messytables.core import RowSet, TableSet, Cell
from messytables.types import StringType, IntegerType, \
        DateType
import messytables


class XLSXTableSet(TableSet):
    """ An excel workbook wrapper object. As the underlying
    library is based on reading from a file name (as opposed to
    a file object), a local, temporary copy is created and
    passed into the library. This has significant performance
    implication for large excel sheets. """

    def __init__(self, fileobj, window=None):
        '''Initialize the object.

        :param fileobj: may be a file path or a file-like object. Note the
        file-like object *must* be in binary mode and must be seekable (it will
        get passed to zipfile).

        As a specific tip: urllib2.urlopen returns a file-like object that is
        not in file-like mode while urllib.urlopen *does*!

        To get a seekable file you *cannot* use
        messytables.core.seekable_stream as it does not support the full seek
        functionality.
        '''
        if hasattr(fileobj, 'read'):
            # wrap in a StringIO so we do not have hassle with seeks and binary etc
            # (see notes to __init__ above)
            # TODO: rather wasteful if in fact fileobj comes from disk
            fileobj = cStringIO.StringIO(fileobj.read())
        self.window = window
        # looking at the openpyxl source code shows filename argument may be a fileobj
        self.workbook = load_workbook(fileobj)

    @property
    def tables(self):
        """ Return the sheets in the workbook. """
        return [XLSXRowSet(sheet, self.window) for sheet in self.workbook.worksheets]


class XLSXRowSet(RowSet):
    """ Excel support for a single sheet in the excel workbook. Unlike
    the CSV row set this is not a streaming operation. """

    def __init__(self, sheet, window=None):
        self.name = sheet.title
        self.sheet = sheet
        self.window = window or 1000
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
