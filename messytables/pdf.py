from messytables.core import RowSet, TableSet, Cell
try:
    from pdftables import get_tables
except ImportError:
    get_tables = None


class PDFTableSet(TableSet):
    def __init__(self, fileobj=None, filename=None):
        if get_tables is None:
            raise ImportError("pdftables is not installed")
        if filename is not None:
            self.fh = open(filename, 'r')
        elif fileobj is not None:
            self.fh = fileobj
        else:
            raise TypeError('You must provide one of filename or fileobj')
        self.raw_tables = get_tables(self.fh)

    @property
    def tables(self):
        def table_name(table):
            return "Table {0} of {1} on page {2} of {3}".format(
                table.table_number_on_page + 1,
                table.total_tables_on_page,
                table.page_number + 1,
                table.total_pages)
        return [PDFRowSet(table_name(table), table)
                for table in self.raw_tables]


class PDFRowSet(RowSet):

    def __init__(self, name, table):
        if get_tables is None:
            raise ImportError("pdftables is not installed")
        super(PDFRowSet, self).__init__()
        self.name = name
        self.table = table

    def raw(self, sample=False):
        """
        Yield one row of cells at a time
        """
        for row in self.table:
            yield [Cell(pdf_cell) for pdf_cell in row]
