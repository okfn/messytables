from messytables.core import RowSet, TableSet, Cell
import lxml.html
import json


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


class HTMLRowSet(RowSet):
    def __init__(self, name, sheet, window=None):
        self.name = name
        self.sheet = sheet
        self.window = window or 1000
        super(HTMLRowSet, self).__init__()

    def raw(self, sample=False):
        # TODO handle rowspan/colspan gracefully.
        for row in self.sheet.xpath('.//tr'):
            # TODO: handle header nicer - preserve the fact it's a header!
            yield [Cell(cell.text_content())
                   for cell in row.xpath('.//*[name()="td" or name()="th"]')]
