from messytables.core import RowSet, TableSet, Cell
import lxml.html
import json
from collections import Counter


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
    [1, None, None, None, 2, 3]
    """
    for i in skip:
        row.insert(i,Cell("NOPE"))
    return row


class HTMLRowSet(RowSet):
    def __init__(self, name, sheet, window=None):
        self.name = name
        self.sheet = sheet
        self.window = window or 1000
        super(HTMLRowSet, self).__init__()

    def raw(self, sample=False):
        toskip = Counter()
        # TODO handle rowspan/colspan gracefully.
        for row in self.sheet.xpath('.//tr'):
            # TODO: handle header nicer - preserve the fact it's a header!
            htmlcells = row.xpath('.//*[name()="td" or name()="th"]')
            items = [Cell(cell.text_content()) for cell in htmlcells]
            column = 0
            """ at the end of this chunk, you will have an accurate toskip."""
            # Add rowspan(or 1) to the next colspan(or 1) columns.
            # Except the current column; add one less to that.
            for htmlcell in htmlcells:
                while column in toskip.keys():
                    column = column+1
                rowspan = int(htmlcell.attrib.get('rowspan', "1"))
                colspan = int(htmlcell.attrib.get('colspan', "1"))
                skipdict = Counter({x: rowspan for x in range(column, column + colspan)})  # fullbox
                skipdict.subtract(Counter([column]))  # remove top left cell
                toskip = toskip + skipdict  # this also normalises
            yield skipcols(items, toskip.keys())
            toskip.subtract(toskip.keys())
            toskip = toskip + Counter()  # normalise: remove <=0
