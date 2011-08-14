import os
import unittest
import decimal
from datetime import datetime
from pprint import pprint

def horror_fobj(name):
    fn = os.path.join(os.path.dirname(__file__), '..', 'horror', name)
    return open(fn, 'rb')

from messytables import *

class RowSetTestCase(unittest.TestCase):

    def test_read_simple_csv(self):
        fh = horror_fobj('simple.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        assert 7 == len(list(row_set))
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
        row_set = table_set.tables[0]
        row = list(row_set.sample)[0]
        assert row[0].value == 'date'
        assert row[1].value == 'temperature'
        
        for row in list(row_set):
            assert 3 == len(row), row

    def test_read_head_padding_csv(self):
        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        offset, headers = headers_guess(row_set.sample)
        assert 11==len(headers), headers
        assert '1985'==headers[1], headers
        row_set.register_processor(headers_processor(headers))
        row_set.register_processor(offset_processor(offset + 1))
        for row in row_set:
            assert 11==len(row), row

    def test_guess_headers(self):
        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        offset, headers = headers_guess(row_set.sample)
        row_set.register_processor(headers_processor(headers))
        row_set.register_processor(offset_processor(offset + 1))
        data = list(row_set)
        assert 'Chirurgie' in data[0][0].value, \
            data[0][0].value
        
        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        row_set.register_processor(headers_processor(['foo', 'bar']))
        data = list(row_set)
        assert 'foo' in data[12][0].column, data[12][0]
        assert 'Chirurgie' in data[12][0].value, \
            data[12][0].value

    def test_read_type_guess_simple(self):
        fh = horror_fobj('simple.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        types = type_guess(row_set.sample)
        expected_types = [DateType("%Y-%m-%d"),DecimalType(),StringType()]
        assert types==expected_types, types

        row_set.register_processor(types_processor(types))
        data = list(row_set)
        header_types = map(lambda c: c.type, data[0])
        assert header_types==[StringType()]*3, header_types
        row_types = map(lambda c: c.type, data[2])
        assert expected_types==row_types, row_types
    
    def test_read_type_know_simple(self):
        fh = horror_fobj('simple.xls')
        table_set = XLSTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        row = list(row_set.sample)[1]
        types = [c.type for c in row]
        assert types==[DateType(None),IntegerType(),StringType()], types

if __name__ == '__main__':
    unittest.main()
