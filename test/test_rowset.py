import os
import unittest
import decimal
from datetime import datetime
from pprint import pprint
import StringIO


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
            assert row[0].type == StringType()

    def test_read_complex_csv(self):
        fh = horror_fobj('complex.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        assert 4 == len(list(row_set))
        row = list(row_set.sample)[0]
        assert row[0].value == 'date'
        assert row[1].value == 'another date'
        assert row[2].value == 'temperature'
        assert row[3].value == 'place'

        for row in list(row_set):
            assert 4 == len(row), row
            assert row[0].type == StringType()

    def test_read_simple_tsv(self):
        fh = horror_fobj('example.tsv')
        table_set = CSVTableSet.from_fileobj(fh, delimiter='\t')
        row_set = table_set.tables[0]
        assert 141 == len(list(row_set)), len(list(row_set))
        row = list(row_set.sample)[0]
        assert row[0].value == 'hour', row[0].value
        assert row[1].value == 'expr1_0_imp', row[1].value
        for row in list(row_set):
            assert 17 == len(row), len(row)
            assert row[0].type == StringType()

    def test_read_simple_xls(self):
        fh = horror_fobj('simple.xls')
        table_set = XLSTableSet.from_fileobj(fh)
        assert 1 == len(table_set.tables)
        row_set = table_set.tables[0]
        row = list(row_set.sample)[0]
        assert row[0].value == 'date'
        assert row[1].value == 'temperature'
        assert row[2].value == 'place'

        for row in list(row_set):
            assert 3 == len(row), row

    def test_read_simple_xlsx(self):
        fh = horror_fobj('simple.xlsx')
        table_set = XLSXTableSet.from_fileobj(fh)
        assert 1 == len(table_set.tables)
        row_set = table_set.tables[0]
        row = list(row_set.sample)[0]
        assert row[0].value == 'date'
        assert row[1].value == 'temperature'
        assert row[2].value == 'place'

        for row in list(row_set):
            assert 3 == len(row), row

    def test_read_head_padding_csv(self):
        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        offset, headers = headers_guess(row_set.sample)
        assert 5 == len(headers), headers
        assert u'Region' == headers[1].strip(), headers[1]
        row_set.register_processor(headers_processor(headers))
        row_set.register_processor(offset_processor(offset + 1))
        for row in row_set:
            assert 5 == len(row), row

    def test_read_head_offset_csv(self):
        fh = horror_fobj('simple.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        offset, headers = headers_guess(row_set.sample)
        assert offset == 0, offset
        row_set.register_processor(offset_processor(offset + 1))
        data = list(row_set.sample)
        assert int(data[0][1].value) == 1, data[0][1].value
        data = list(row_set)
        assert int(data[0][1].value) == 1, data[0][1].value

    def test_read_head_offset_excel(self):
        fh = horror_fobj('simple.xls')
        table_set = XLSTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        offset, headers = headers_guess(row_set.sample)
        assert offset == 0, offset
        row_set.register_processor(offset_processor(offset + 1))
        data = list(row_set.sample)
        assert int(data[0][1].value) == 1, data[0][1].value
        data = list(row_set)
        assert int(data[0][1].value) == 1, data[0][1].value

    def test_guess_headers(self):
        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        offset, headers = headers_guess(row_set.sample)
        row_set.register_processor(headers_processor(headers))
        row_set.register_processor(offset_processor(offset + 1))
        data = list(row_set)
        assert 'Chirurgie' in data[9][0].value, \
            data[9][0].value

        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        row_set.register_processor(headers_processor(['foo', 'bar']))
        data = list(row_set)
        assert 'foo' in data[12][0].column, data[12][0]
        assert 'Chirurgie' in data[12][0].value, \
            data[12][0].value

    def test_type_guess(self):
        csv_file = StringIO.StringIO('''
            1,   2012/2/12, 2,   02 October 2011
            2,   2012/2/12, 2,   02 October 2011
            2.4, 2012/2/12, 1, 1 May 2011
            foo, bar,       1000,
            4.3, ,          42,  24 October 2012
             ,   2012/2/12, 21,  24 December 2013''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows)

        assert guessed_types == [DecimalType(), DateType('%Y/%m/%d'), IntegerType(), DateType('%d %B %Y')], guessed_types

    def test_type_guess_strict(self):
        csv_file = StringIO.StringIO('''
            1,   2012/2/12, 2,   2, 02 October 2011
            2,   2012/2/12, 1.1,  , 1 May 2011
            foo, bar,       "1500",   0,
            4,   2012/2/12, 42,  -2, 24 October 2012''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows, strict=True)
        assert guessed_types == [StringType(), StringType(), DecimalType(), IntegerType(), DateType('%d %B %Y')], guessed_types

    def test_read_type_guess_simple(self):
        fh = horror_fobj('simple.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        types = type_guess(row_set.sample)
        expected_types = [DateType("%Y-%m-%d"), IntegerType(), StringType()]
        assert types == expected_types, types

        row_set.register_processor(types_processor(types))
        data = list(row_set)
        header_types = map(lambda c: c.type, data[0])
        assert header_types == [StringType()] * 3, header_types
        row_types = map(lambda c: c.type, data[2])
        assert expected_types == row_types, row_types

    def test_read_type_know_simple(self):
        fh = horror_fobj('simple.xls')
        table_set = XLSTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        row = list(row_set.sample)[1]
        types = [c.type for c in row]
        assert types == [DateType(None), IntegerType(), StringType()], types

    def test_read_encoded_csv(self):
        fh = horror_fobj('utf-16le_encoded.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        assert 328 == len(list(row_set)), len(list(row_set))
        row = list(row_set.sample)[0]
        assert row[1].value == 'Organisation_name', row[1].value

    def test_long_csv(self):
        fh = horror_fobj('long.csv')
        table_set = CSVTableSet.from_fileobj(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert 4000 == len(data), len(data)

if __name__ == '__main__':
    unittest.main()
