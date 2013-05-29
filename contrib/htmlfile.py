from messytables.core import RowSet, TableSet, Cell
from messytables.types import StringType, IntegerType, DateType, FloatType
import lxml.html
import requests

class HTMLTableSet(TableSet):
    def __init__(self, fileobj=None, filename=None, window=None, encoding=None):
        
        if filename:
            fh = open(filename, 'r')
        else:
            fh = fileobj
        if not fh:
            raise Exception('You must provide one of filename or fileobj')

        self.htmltables = lxml.html.fromstring(fh.read()).xpath('//table')
        #html = requests.get('http://unstats.un.org/unsd/methods/m49/m49alpha.htm').content
        #self.htmltables = lxml.html.fromstring(html)

    @property
    def tables(self):
        return [HTMLRowSet('test', x) for x in self.htmltables]


class HTMLRowSet(RowSet):
    def __init__(self, name, sheet, window=None):  # TODO verify all required
        self.name = name
        self.sheet = sheet
        self.window = window or 1000
        super(HTMLRowSet, self).__init__(typed=True)  # TODO turn off typing?

    def raw(self, sample=False):
        # TODO handle rowspan/colspan gracefully.
        for row in self.sheet.xpath('.//tr'):
            # TODO: handle header nicer
            yield [Cell(cell.text_content(), StringType) for cell in row.xpath('.//*[name()="td" or name()="th"]')]
