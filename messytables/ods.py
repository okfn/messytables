import cStringIO
import itertools
import re
import zipfile

import dateutil.parser as parser
from lxml import etree

from messytables.core import RowSet, TableSet, Cell
from messytables.error import ReadError
from messytables.types import (StringType, IntegerType, FloatType,
                               DecimalType, DateType, DateUtilType)


ODS_TABLE_MATCH = re.compile(".*?(<table:table.*?<\/.*?:table>).*?", re.MULTILINE)
ODS_TABLE_NAME = re.compile('.*?table:name=\"(.*?)\".*?')
ODS_ROW_MATCH = re.compile(".*?(<table:table-row.*?<\/.*?:table-row>).*?", re.MULTILINE)

ODS_TYPES = {
    'float': FloatType(),
    'date': DateType(None),
}

class ODSTableSet(TableSet):
    """
    A wrapper around ODS files. Because they are zipped and the info we want
    is in the zipped file as content.xml we must ensure that we either have
    a seekable object (local file) or that we retrieve all of the content from
    the remote URL.
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

        with zipfile.ZipFile(fileobj).open("content.xml") as fp:
            self.content =  fp.read()

    @property
    def tables(self):
        """
            Return the sheets in the workbook.

            A regex is used for this to avoid having to:

            1. load large the entire file into memory, or
            2. SAX parse the file more than once
        """
        sheets = [m.groups(0)[0] for m in ODS_TABLE_MATCH.finditer(self.content)]
        return [ODSRowSet(sheet, self.window) for sheet in sheets]


class ODSRowSet(RowSet):
    """ ODS support for a single sheet in the ODS workbook. Unlike
    the CSV row set this is not a streaming operation. """

    def __init__(self, sheet, window=None):
        self.sheet = sheet

        self.name = "Unknown"
        m = ODS_TABLE_NAME.match(self.sheet)
        if m:
            self.name = m.groups(0)[0]

        self.window = window or 1000
        super(ODSRowSet, self).__init__(typed=True)

    def raw(self, sample=False):
        """ Iterate over all rows in this sheet. """
        rows = ODS_ROW_MATCH.findall(self.sheet)

        for row in rows:
            row_data = []

            partial = cStringIO.StringIO(row)
            context = etree.iterparse(partial, ('end',))

            for action, elem in context:
                if elem.tag == 'table-cell':
                    cell_type = elem.attrib.get('value-type')
                    children = elem.getchildren()
                    if children:
                        c = Cell(children[0].text, type=ODS_TYPES.get(cell_type, StringType()))
                        row_data.append(c)

            if not row_data:
                raise StopIteration()

            del context
            del partial
            yield row_data
        del rows

