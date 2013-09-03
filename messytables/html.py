from messytables.core import RowSet, TableSet, Cell, CoreProperties
import lxml.html
from collections import defaultdict


class HTMLTableSet(TableSet):
    def __init__(self, fileobj=None, filename=None, window=None):

        if filename:
            fh = open(filename, 'r')
        else:
            fh = fileobj
        if not fh:
            raise TypeError('You must provide one of filename or fileobj')

        self.htmltables = []
        root = lxml.html.fromstring(fh.read())

        # Grab tables that don't contain tables, remove from root, repeat.
        while True:
            dropped = False
            tables = root.xpath('//table[not(@messytable)]')
            if not tables:
                break
            for t in tables:
                if not t.xpath(".//table[not(@messytable)]"):
                    self.htmltables.append(t)
                    t.attrib['messytable'] = 'done'
                    dropped = True
            assert dropped, "Didn't find any tables not containing " + \
                "other tables. This is a bug."  # avoid infinite loops

    @property
    def tables(self):
        def rowset_name(rowset, table_index):
            return "Table {0} of {1}".format(table_index + 1,
                                             len(self.htmltables))

        return [HTMLRowSet(rowset_name(rowset, index), rowset)
                for index, rowset in enumerate(self.htmltables)]


def insert_blank_cells(row, blanks):
    """
    Given a list of values, insert blank cells at the indexes given by blanks
    The letters in these examples should really be cells.
    >>> insert_blank_cells(["a","e","f"],[1,2,3])
    ['a', <Cell(String:>, <Cell(String:>, <Cell(String:>, 'e', 'f']
    """
    # DISCUSS: option to repeat top-left of col/rowspan.
    # or to identify that areas are a single cell, originally.
    for i in blanks:
        row.insert(i, FakeHTMLCell())
    return row


class HTMLRowSet(RowSet):
    def __init__(self, name, sheet, window=None):
        self.name = name
        self.sheet = sheet
        self.window = window or 1000
        super(HTMLRowSet, self).__init__()

    def in_table(self, els):
        """
        takes a list of xpath elements and returns only those
        whose parent table is this one
        """

        return [e for e in els
                if self.sheet in e.xpath("./ancestor::table[1]")]

    def raw(self, sample=False):
        def identify_anatomy(tag):
            # 0: thead, 1: tbody, 2: tfoot
            parts = ['.//ancestor::thead',
                     './/ancestor::tbody',
                     './/ancestor::tfoot']
            for i, part in enumerate(parts):
                if self.in_table(tag.xpath(part)):
                    return i
            return 2  # default to body

        blank_cells = defaultdict(list)  # ie row 2, cols 3,4,6: {2: [3,4,6]}
        allrows = sorted(self.in_table(self.sheet.xpath(".//tr")),
                         key=lambda tag: identify_anatomy(tag))
        # http://stackoverflow.com/questions/1915376/ - sorted() is stable.

        for r, row in enumerate(allrows):
            # TODO: handle header nicer - preserve the fact it's a header!
            html_cells = self.in_table(
                row.xpath('.//*[name()="td" or name()="th"]'))

            """ at the end of this chunk, you have accurate blank_cells."""
            output_column = 0
            for html_cell in html_cells:
                assert type(r) == int
                while output_column in blank_cells[r]:
                    output_column += 1  # pass over col, doesn't exist in src
                rowspan = int(html_cell.attrib.get('rowspan', "1"))
                colspan = int(html_cell.attrib.get('colspan', "1"))
                x_range = range(output_column, output_column + colspan)
                y_range = range(r, r + rowspan)
                for x in x_range:
                    for y in y_range:
                        if (output_column, r) != (x, y):
                            # don't skip current cell
                            blank_cells[y].append(x)
                output_column += 1

            cells = [HTMLCell(source=cell) for cell in html_cells]
            yield insert_blank_cells(cells, blank_cells[r])
            if sample and r == self.window:
                return
            del blank_cells[r]


class FakeHTMLCell(Cell):
    def __init__(self):
        super(FakeHTMLCell, self).__init__("")


class HTMLCell(Cell):
    """ The Cell __init__ signature is:
    def __init__(self, value=None, column=None, type=None):
    where 'value' is the primary input, 'column' is a column name, and
    type is messytables.types.StringType() or better."""

    def __init__(self, value=None, column=None, type=None, source=None):
        assert value is None
        assert isinstance(source, lxml.html.HtmlElement)
        self._lxml = source
        if type is None:
            from messytables.types import StringType
            type = StringType()
        self.type = type
        self.column = column
        self.column_autogenerated = False

    @property
    def value(self):
        return self._lxml.text_content()  # TODO improve?

    @property
    def properties(self):
        return HTMLProperties(self._lxml)


class HTMLProperties(CoreProperties):
    KEYS = ['_lxml', 'html']

    def __init__(self, lxml_element):
        if not isinstance(lxml_element, lxml.html.HtmlElement):
            raise TypeError("%r" % lxml_element)
        super(HTMLProperties, self).__init__()
        self.lxml_element = lxml_element

    def get_html(self):
        return lxml.html.tostring(self.lxml_element)

    def get__lxml(self):
        return self.lxml_element
