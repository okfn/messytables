from messytables.core import RowSet, TableSet, Cell
import lxml.html
import json
from collections import defaultdict


class HTMLTableSet(TableSet):
    def __init__(self, fileobj=None, filename=None, window=None):

        if filename:
            fh = open(filename, 'r')
        else:
            fh = fileobj
        if not fh:
            raise TypeError('You must provide one of filename or fileobj')

        self.htmltables = lxml.html.fromstring(fh.read()).xpath('//table')

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
        self.name = name
        self.sheet = sheet
        self.window = window or 1000
        super(HTMLRowSet, self).__init__()

    def raw(self, sample=False):
        blank_cells = defaultdict(list)  # ie row 2, cols 3,4,6: {2: [3,4,6]}
        for r, row in enumerate(self.sheet.xpath('.//tr')):
            # TODO: handle header nicer - preserve the fact it's a header!
            html_cells = row.xpath('.//*[name()="td" or name()="th"]')
            # TODO: only select those that are not children of subtables?

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
