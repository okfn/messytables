import lxml.etree
from collections import defaultdict

import get_abbyy
from messytables.core import RowSet, TableSet, Cell
from get_abbyy import ABBYYAuthError

class ABBYYTableSet(TableSet):
    def __init__(self, fileobj=None, filename=None, window=None):

        if filename:
            fh = open(filename, 'rb')
        else:
            fh = fileobj
        if not fh:
            raise TypeError('You must provide one of filename or fileobj')

        xml = get_abbyy.OCRFile(fh).get_ocr_content()
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
                row.insert(i, Cell("", properties={'span': True}))
            return row

        def makecell(xmlcell):
            linebuilder=[]
            for line in xmlcell.xpath(".//*[local-name()='line']"):
                linebuilder.append(''.join(line.xpath(".//*[local-name()='charParams']/text()")))
            return Cell('\n'.join(linebuilder))

        blank_cells = defaultdict(list)  # ie row 2, cols 3,4,6: {2: [3,4,6]}
        for r, row in enumerate(self.sheet.xpath(".//*[local-name()='row']")):
            xml_cells = row.xpath(".//*[local-name()='cell']")

            # at the end of this chunk, you will have accurate blank_cells
            output_column = 0
            for xml_cell in xml_cells:
                while output_column in blank_cells[r]:
                    output_column += 1  # pass over col, not in source table
                rowspan = int(xml_cell.attrib.get('rowSpan', "1"))
                colspan = int(xml_cell.attrib.get('colSpan', "1"))
                x_range = range(output_column, output_column + colspan)
                y_range = range(r, r + rowspan)
                for x in x_range:
                    for y in y_range:
                        if (output_column, r) != (x, y):
                            # don't skip current cell
                            blank_cells[y].append(x)
                output_column += 1

            cells = [makecell(xmlcell) for xmlcell in xml_cells]
            yield insert_blank_cells(cells, blank_cells[r])
            if sample and r == self.window:
                return
            del blank_cells[r]
