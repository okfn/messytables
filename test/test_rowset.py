import os
import unittest
import decimal
from datetime import datetime
from pprint import pprint

def horror_fobj(name):
    fn = os.path.join(os.path.dirname(__file__), '..', 'horror', name)
    return open(fn, 'rb')

#from messytables.basic import CSVTableSet, XLSTableSet, 
from messytables.basic import *

class RowSetTestCase(unittest.TestCase):

    def test_read_simple_csv(self):
        fh = horror_fobj('simple.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0][1]
        assert 7 == len(list(row_set.rows()))
        row = list(row_set.sample)[0]
        assert row[0].value == 'date'
        assert row[1].value == 'temperature'

        for row in list(row_set):
            assert 3 == len(row), row
            assert row[0].type==StringType()

    def test_read_simple_xls(self):
        fh = horror_fobj('simple.xls')
        table_set = XLSTableSet.from_fileobj(fh)
        assert 1==len(table_set.tables)
        row_set = table_set.tables[0][1]
        row = list(row_set.sample)[0]
        assert row[0].value == 'date'
        assert row[1].value == 'temperature'
        
        for row in list(row_set):
            assert 3 == len(row), row

    def test_read_head_padding_csv(self):
        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0][1]
        column_count = row_set.sample_modal_column_count
        offset, headers = find_header_by_count(row_set, column_count-1)
        assert 11==len(headers), headers
        assert '1985'==headers[1], headers
        for row in apply_headers(headers, skip_n(iter(row_set), offset+1)):
            assert 11==len(row), row

    def test_dictize_rowset(self):
        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0][1]
        data = list(dictize_rowset(row_set))
        assert 'Chirurgie' in data[0][0].value, \
            data[0][0].value
        
        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0][1]
        data = list(dictize_rowset(row_set, header_offset=7))
        assert 'Geschlecht' in data[4][0].column, data[4][0].column
        assert 'Chirurgie' in data[4][0].value, \
            data[4][0].value
        
        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0][1]
        data = list(dictize_rowset(row_set, headers=['foo', 'bar']))
        assert 'foo' in data[11][0].column, data[11][0]
        assert 'Chirurgie' in data[11][0].value, \
            data[11][0].value

    def test_read_type_guess_simple(self):
        fh = horror_fobj('simple.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0][1]
        types = type_guess(row_set.sample)
        assert types==[DateType("%Y-%m-%d"),DecimalType(),StringType()], types
    
    def test_read_type_know_simple(self):
        fh = horror_fobj('simple.xls')
        table_set = XLSTableSet.from_fileobj(fh)
        row_set = table_set.tables[0][1]
        row = list(row_set.sample)[1]
        types = [c.type for c in row]
        assert types==[DateType(None),IntegerType(),StringType()], types

if __name__ == '__main__':
    unittest.main()
