from messytables.core import RowSet, TableSet, Cell
import lxml.html
import json
from collections import Counter, defaultdict


class HTMLTableSet(TableSet):
    def __init__(self, fileobj=None, filename=None, window=None, encoding=None):

        if filename:
            fh = open(filename, 'r')
        else:
            fh = fileobj
        if not fh:
            raise Exception('You must provide one of filename or fileobj')

        self.htmltables = lxml.html.fromstring(fh.read()).xpath('//table')

    @property
    def tables(self):
        return [HTMLRowSet(json.dumps(dict(x.attrib)), x) for x in self.htmltables]


def skipcols(row, skip):
    """
    >>> skipcols([1,2,3],[1,2,3])
    [1, <Cell(String:>, <Cell(String:>, <Cell(String:>, 2, 3]
    >>> skipcols([1,2,4],[2])
    [1, 2, <Cell(String:>, 4]
    """
    print "***", skip
    for i in skip:
        if i >= 0:  # TODO: negative hack!
            row.insert(i, Cell(""))  # TODO: option to use top-left of col/rowspan.
    return row


class HTMLRowSet(RowSet):
    def __init__(self, name, sheet, window=None):
        self.name = name
        self.sheet = sheet
        self.window = window or 1000
        super(HTMLRowSet, self).__init__()

    def raw(self, sample=False):
        toskip = defaultdict(list)
        assert "Element" not in repr(toskip), toskip
        # TODO handle rowspan/colspan gracefully.
        for r, row in enumerate(self.sheet.xpath('.//tr')):
            # TODO: handle header nicer - preserve the fact it's a header!
            htmlcells = row.xpath('.//*[name()="td" or name()="th"]')
            items = [Cell(cell.text_content()) for cell in htmlcells]
            column = 0
            """ at the end of this chunk, you will have an accurate toskip."""
            # Add rowspan(or 1) to the next colspan(or 1) columns.
            # Except the current column; add one less to that.
            for htmlcell in htmlcells:
                assert type(r) == int
                while column in toskip[r]:
                    column = column+1
                rowspan = int(htmlcell.attrib.get('rowspan', "1"))
                colspan = int(htmlcell.attrib.get('colspan', "1"))
                x_range = range(column, column + colspan)
                y_range = range(r, r + rowspan)
                if rowspan + colspan > 2:
                    print "span at %r, %r: %r, %r"%(r, column, rowspan, colspan)
                for x in x_range:
                    for y in y_range:
                        if column != x or r != y:
                            print "blank %r, %r"%(x,y)
                            toskip[y].append(x)
                            print toskip

                column = column + 1
            
            yield skipcols(items, toskip[r])
