from messytables.core import RowSet, TableSet, Cell
import lxml.etree
import get_abbyy


class ABBYYTableSet(TableSet):
    def __init__(self, fileobj=None, filename=None, window=None):

        if filename:
            fh = open(filename, 'rb')
        else:
            fh = fileobj
        if not fh:
            raise TypeError('You must provide one of filename or fileobj')

        xml = get_abbyy.OCR(fh).get_ocr_content()
        root = lxml.etree.fromstring(xml)
        self.xmltables = root.xpath("//*[local-name()='block' and @blockType='Table']")

    @property
    def tables(self):
        def rowset_name(rowset, table_index):
            return "Table {0} of {1}".format(table_index + 1,
                                             len(self.xmltables))

        return [ABBYYRowSet(rowset_name(rowset, index), rowset)
                for index, rowset in enumerate(self.xmltables)]


class ABBYYRowSet(RowSet):
    def __init__(self, name, sheet, window=None):
        self.name = name
        self.sheet = sheet
        self.window = window or 1000
        super(ABBYYRowSet, self).__init__()

    def raw(self, sample=False):
        # yields rows
        def makecell(xmlcell):
            return Cell(''.join(xmlcell.xpath(".//*[local-name()='charParams']/text()")))

        for row in self.sheet.xpath(".//*[local-name()='row']"):
            yield [makecell(xmlcell) for xmlcell in row.xpath(".//*[local-name()='cell']")]
