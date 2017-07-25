import io
import re
import zipfile

from lxml import etree

from messytables.core import RowSet, TableSet, Cell
from messytables.types import (StringType, DecimalType,
                               DateType, BoolType, CurrencyType,
                               TimeType, PercentageType)


ODS_NAMESPACES_TAG_MATCH = re.compile(
    b"(<office:document-content[^>]*>)", re.MULTILINE)
ODS_TABLE_MATCH = re.compile(
    b".*?(<table:table.*?<\/.*?:table>).*?", re.MULTILINE)
ODS_TABLE_NAME = re.compile(b'.*?table:name=\"(.*?)\".*?')
ODS_ROW_MATCH = re.compile(
    b".*?(<table:table-row.*?<\/.*?:table-row>).*?", re.MULTILINE)

NS_OPENDOCUMENT_PTTN = u"urn:oasis:names:tc:opendocument:xmlns:%s"
NS_CAL_PTTN = u"urn:org:documentfoundation:names:experimental:calc:xmlns:%s"
NS_OPENDOCUMENT_TABLE = NS_OPENDOCUMENT_PTTN % "table:1.0"
NS_OPENDOCUMENT_OFFICE = NS_OPENDOCUMENT_PTTN % "office:1.0"

TABLE_CELL = 'table-cell'
VALUE_TYPE = 'value-type'
COLUMN_REPEAT = 'number-columns-repeated'
EMPTY_CELL_VALUE = ''

ODS_VALUE_TOKEN = {
    "float": "value",
    "date": "date-value",
    "time": "time-value",
    "boolean": "boolean-value",
    "percentage": "value",
    "currency": "value"
}

ODS_TYPES = {
    'float': DecimalType(),
    'date': DateType('%Y-%m-%d'),
    'boolean': BoolType(),
    'percentage': PercentageType(),
    'time': TimeType()
}


class ODSTableSet(TableSet):
    """
    A wrapper around ODS files. Because they are zipped and the info we want
    is in the zipped file as content.xml we must ensure that we either have
    a seekable object (local file) or that we retrieve all of the content from
    the remote URL.
    """

    def __init__(self, fileobj, window=None, **kw):
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
            fileobj = io.BytesIO(fileobj.read())

        self.window = window

        zf = zipfile.ZipFile(fileobj).open("content.xml")
        self.content = zf.read()
        zf.close()

    def make_tables(self):
        """
            Return the sheets in the workbook.

            A regex is used for this to avoid having to:

            1. load large the entire file into memory, or
            2. SAX parse the file more than once
        """
        namespace_tags = self._get_namespace_tags()
        sheets = [m.groups(0)[0]
                  for m in ODS_TABLE_MATCH.finditer(self.content)]
        return [ODSRowSet(sheet, self.window, namespace_tags)
                for sheet in sheets]

    def _get_namespace_tags(self):
        match = re.search(ODS_NAMESPACES_TAG_MATCH, self.content)
        assert match
        tag_open = match.groups()[0]
        tag_close = b'</office:document-content>'
        return tag_open, tag_close


class ODSRowSet(RowSet):
    """ ODS support for a single sheet in the ODS workbook. Unlike
    the CSV row set this is not a streaming operation. """

    def __init__(self, sheet, window=None, namespace_tags=None):
        self.sheet = sheet

        self.name = "Unknown"
        m = ODS_TABLE_NAME.match(self.sheet)
        if m:
            self.name = m.groups(0)[0]

        self.window = window or 1000

        # We must wrap the XML fragments in a valid header otherwise iterparse
        # will explode with certain (undefined) versions of libxml2. The
        # namespaces are in the ODS file, and change with the libreoffice
        # version saving it, so get them from the ODS file if possible. The
        # default namespaces are an option to preserve backwards compatibility
        # of ODSRowSet.
        if namespace_tags:
            self.namespace_tags = namespace_tags
        else:
            namespaces = {
                "dc": u"http://purl.org/dc/elements/1.1/",
                "draw": NS_OPENDOCUMENT_PTTN % u"drawing:1.0",
                "number": NS_OPENDOCUMENT_PTTN % u"datastyle:1.0",
                "office": NS_OPENDOCUMENT_PTTN % u"office:1.0",
                "svg": NS_OPENDOCUMENT_PTTN % u"svg-compatible:1.0",
                "table": NS_OPENDOCUMENT_PTTN % u"table:1.0",
                "text": NS_OPENDOCUMENT_PTTN % u"text:1.0",
                "calcext": NS_CAL_PTTN % u"calcext:1.0",
            }

            ods_header = u"<wrapper {0}>"\
                .format(" ".join('xmlns:{0}="{1}"'.format(k, v)
                        for k, v in namespaces.iteritems())).encode('utf-8')
            ods_footer = u"</wrapper>".encode('utf-8')
            self.namespace_tags = (ods_header, ods_footer)

        super(ODSRowSet, self).__init__(typed=True)

    def raw(self, sample=False):
        """ Iterate over all rows in this sheet. """
        rows = ODS_ROW_MATCH.findall(self.sheet)

        for row in rows:
            row_data = []

            block = self.namespace_tags[0] + row + self.namespace_tags[1]
            partial = io.BytesIO(block)
            empty_row = True

            for action, element in etree.iterparse(partial, ('end',)):
                if element.tag != _tag(NS_OPENDOCUMENT_TABLE, TABLE_CELL):
                    continue

                cell = _read_cell(element)
                if empty_row is True and cell.value != EMPTY_CELL_VALUE:
                    empty_row = False

                repeat = element.attrib.get(
                    _tag(NS_OPENDOCUMENT_TABLE, COLUMN_REPEAT))
                if repeat:
                    number_of_repeat = int(repeat)
                    row_data += [cell] * number_of_repeat
                else:
                    row_data.append(cell)

            if empty_row:
                # ignore blank lines
                continue

            del partial
            yield row_data
        del rows


def _read_cell(element):
    cell_type = element.attrib.get(_tag(NS_OPENDOCUMENT_OFFICE, VALUE_TYPE))
    value_token = ODS_VALUE_TOKEN.get(cell_type, 'value')
    if cell_type == 'string':
        cell = _read_text_cell(element)
    elif cell_type == 'currency':
        value = element.attrib.get(_tag(NS_OPENDOCUMENT_OFFICE, value_token))
        currency = element.attrib.get(_tag(NS_OPENDOCUMENT_OFFICE, 'currency'))
        cell = Cell(value + ' ' + currency, type=CurrencyType())
    elif cell_type is not None:
        value = element.attrib.get(_tag(NS_OPENDOCUMENT_OFFICE, value_token))
        cell = Cell(value, type=ODS_TYPES.get(cell_type, StringType()))
    else:
        cell = Cell(EMPTY_CELL_VALUE, type=StringType())

    return cell


def _read_text_cell(element):
    children = element.getchildren()
    text_content = []
    for child in children:
        if child.text:
            text_content.append(child.text)
        else:
            text_content.append(EMPTY_CELL_VALUE)
    if len(text_content) > 0:
        cell_value = '\n'.join(text_content)
    else:
        cell_value = EMPTY_CELL_VALUE
    return Cell(cell_value, type=StringType())


def _tag(namespace, tag):
    return '{%s}%s' % (namespace, tag)
