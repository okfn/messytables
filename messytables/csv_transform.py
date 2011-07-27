"""
Data Proxy - CSV transformation adapter
"""
import base
import csv
import csv_file
# import brewery.ds as ds

class CSVDataSource(object):
    """
    A wrapper around the csv_file module that makes it available as a
    Brewery DataSource.
    See http://packages.python.org/brewery/stores.html for more info.

    Todo:

        * Should csv_file.CsvFile take a file object instead of a path?
        * implement DataSource records() method
    """
    def __init__(self, handle, encoding=None, dialect=None):
        self.csv_file = csv_file.CsvFile(handle)
        self.encoding = encoding
        self.dialect = dialect
        self.field_names = []
        self.data = []

    def initialize(self):
        try:
            self.csv_file.guess_skip_lines()
            self.csv_file.get_dialect()
            self.csv_file.get_headings()
            self.csv_file.parse_headings()
            # TODO: disable type guessing for now, can be quite slow
            #       and results are not being used by the webstore
            # self.csv_file.guess_types()
        except csv.Error as e:
            print "Error parsing CSV file:", e.message
            return

        # save column names
        self.field_names = self.csv_file.headings

        # save rows to self.data
        errors = 0
        row_num = 0
        for row in self.csv_file.iterate_csv(as_dict = True, convert=True):
            row_num = row_num + 1
            if row['__errors']:
                errors = errors + 1
            # flatten row to a list
            row_list = []
            for heading in self.field_names:
                # TODO: should the type information be passed to webstore here
                #       instead of converting to unicode?
                row_list.append(unicode(row[heading]))
            self.data.append(row_list)

    def rows(self):
        return self.data

class CSVTransformer(base.Transformer):
    def __init__(self):
        super(CSVTransformer, self).__init__()
        self.requires_size_limit = False
        self.encoding = 'utf-8'
        self.dialect = None
        
    def transform(self, handle):
        # src = ds.CSVDataSource(handle, encoding = self.encoding, dialect = self.dialect)
        src = CSVDataSource(handle, encoding = self.encoding, dialect = self.dialect)
        src.initialize()
        result = self.read_source_rows(src)
        return result
