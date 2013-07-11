from messytables.core import RowSet, TableSet, Cell
import json
from collections import defaultdict
try:
    import lxml.html
    lxml_ok=True
except:
    lxml_ok=False


class HTMLTableSet(TableSet):
    def __init__(self, fileobj=None, filename=None, window=None):
        if not lxml_ok:
            raise ImportError("lxml not installed (lxml.html)")

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
            tables = root.xpath('//table')
            if not tables:
                break
            for t in tables:
                if not t.xpath(".//table"):
                    self.htmltables.append(t)
                    t.drop_tree()
                    dropped = True
            assert dropped, "Didn't find any tables not containing other tables. This is a bug."  # avoid infinite loops

    @property
    def tables(self):
        def rowset_name(x):
            return json.dumps(dict(x.attrib))

        return [HTMLRowSet(rowset_name(x), x) for x in self.htmltables]


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
        row.insert(i, Cell(""))
    return row


class HTMLRowSet(RowSet):
    def __init__(self, name, sheet, window=None):
        if not lxml_ok:
            raise ImportError("lxml not installed (lxml.html)")
        self.name = name
        self.sheet = sheet
        self.window = window or 1000
        super(HTMLRowSet, self).__init__()

    def raw(self, sample=False):
        def identify_anatomy(tag):
            # 0: thead, 1: tbody, 2: tfoot
            parts = ['.//ancestor::thead',
                     './/ancestor::tbody',
                     './/ancestor::tfoot']
            for i, part in enumerate(parts):
                if tag.xpath(part):
                    return i
            return 2 # default to body

        blank_cells = defaultdict(list)  # ie row 2, cols 3,4,6: {2: [3,4,6]}
        allrows = sorted(self.sheet.xpath(".//tr"), key = lambda tag: identify_anatomy(tag))
        # http://stackoverflow.com/questions/1915376/ - sorted() is stable.

        for r, row in enumerate(allrows):
            # TODO: handle header nicer - preserve the fact it's a header!
            html_cells = row.xpath('.//*[name()="td" or name()="th"]')

            """ at the end of this chunk, you will have an accurate blank_cells."""
            output_column = 0
            for html_cell in html_cells:
                assert type(r) == int
                while output_column in blank_cells[r]:
                    output_column += 1  # pass over col, doesn't exist in src table
                rowspan = int(html_cell.attrib.get('rowspan', "1"))
                colspan = int(html_cell.attrib.get('colspan', "1"))
                x_range = range(output_column, output_column + colspan)
                y_range = range(r, r + rowspan)
                for x in x_range:
                    for y in y_range:
                        if (output_column, r) != (x, y):  # don't skip current cell
                            blank_cells[y].append(x)
                output_column += 1

            cells = [Cell(cell.text_content()) for cell in html_cells]
            yield insert_blank_cells(cells, blank_cells[r])
            if sample and r == self.window:
                return
            del blank_cells[r]
