import cStringIO

from messytables.core import RowSet, TableSet, Cell
from messytables.types import (StringType, IntegerType,
                               DateType)
from messytables.error import ReadError

import odf.opendocument
from odf.table import Table, TableRow, TableCell
from odf.text import P

class ODSTableSet(TableSet):
    """
    A wrapper around ODS files. As the underlying library is based on reading
    from a file name (as opposed to a file object), a local, temporary copy
    is created and passed into the library. This has significant performance
    implication for large excel sheets.
    """

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
            # wrap in a StringIO so we do not have hassle with seeks and
            # binary etc (see notes to __init__ above)
            # TODO: rather wasteful if in fact fileobj comes from disk
            fileobj = cStringIO.StringIO(fileobj.read())

        self.window = window
        try:
            self.workbook = odf.opendocument.load(fileobj)
        except Exception, e:
            raise ReadError('Could not open ODS file: %s', e)

    @property
    def tables(self):
        """ Return the sheets in the workbook. """
        return [ODSRowSet(sheet, self.window)
                for sheet in self.workbook.spreadsheet.getElementsByType(Table)]


class ODSRowSet(RowSet):
    """ ODS support for a single sheet in the ODS workbook. Unlike
    the CSV row set this is not a streaming operation. """

    def __init__(self, sheet, window=None):
        self.sheet = sheet
        self.window = window or 1000
        super(ODSRowSet, self).__init__(typed=True)

    def raw(self, sample=False):
        """ Iterate over all rows in this sheet. Types are not yet provided """
        rows = self.sheet.getElementsByType(TableRow)
        num_rows = len(rows)
        max_rows = min(self.window, num_rows) if sample else num_rows

        count = 0
        for row in rows:
            if count == max_rows:
                break
            count += 1

            data_row = []
            cells = row.getElementsByType(TableCell)
            for cell in cells:
                repeat = cell.getAttribute("numbercolumnsrepeated")
                if(not repeat):
                    repeat = 1

                paras = cell.getElementsByType(P)
                content = ""

                for p in paras:
                    for node in p.childNodes:
                        if (node.nodeType == 3):
                            content = content + unicode(node.data)

                if content and content[0] != "#": # ignore comments cells
                    for rr in range(int(repeat)):

                        data_row.append(Cell(content, type=type(content)))

            if(len(data_row)):
                yield data_row

