import io
import re
import zipfile

from lxml import etree

from messytables.core import RowSet, TableSet, Cell
from messytables.types import (StringType, DecimalType,
                               DateType)


ODS_NAMESPACES_TAG_MATCH = re.compile(b"(<office:document-content[^>]*>)", re.MULTILINE)
ODS_TABLE_MATCH = re.compile(b".*?(<table:table.*?<\/.*?:table>).*?", re.MULTILINE)
ODS_TABLE_NAME = re.compile(b'.*?table:name=\"(.*?)\".*?')
ODS_ROW_MATCH = re.compile(b".*?(<table:table-row.*?<\/.*?:table-row>).*?", re.MULTILINE)

ODS_TYPES = {
    'float': DecimalType(),
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
                "draw": u"urn:oasis:names:tc:opendocument:xmlns:drawing:1.0",
                "number": u"urn:oasis:names:tc:opendocument:xmlns:datastyle:1.0",
                "office": u"urn:oasis:names:tc:opendocument:xmlns:office:1.0",
                "svg": u"urn:oasis:names:tc:opendocument:xmlns:svg-compatible:1.0",
                "table": u"urn:oasis:names:tc:opendocument:xmlns:table:1.0",
                "text": u"urn:oasis:names:tc:opendocument:xmlns:text:1.0",
                "calcext": u"urn:org:documentfoundation:names:experimental:calc:xmlns:calcext:1.0",
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

            for action, elem in etree.iterparse(partial, ('end',)):
                if elem.tag == '{urn:oasis:names:tc:opendocument:xmlns:table:1.0}table-cell':
                    cell_type = elem.attrib.get('urn:oasis:names:tc:opendocument:xmlns:office:1.0:value-type')
                    children = elem.getchildren()
                    if children:
                        c = Cell(children[0].text,
                                 type=ODS_TYPES.get(cell_type, StringType()))
                        row_data.append(c)

            if not row_data:
                # ignore blank lines
                continue

            del partial
            yield row_data
        del rows
