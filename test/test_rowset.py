# -*- coding: utf-8 -*-
import os
import unittest
import StringIO
import urllib2
import requests

from nose.tools import assert_equal
from httpretty import HTTPretty
from httpretty import httprettified


def horror_fobj(name):
    fn = os.path.join(os.path.dirname(__file__), '..', 'horror', name)
    return open(fn, 'rb')

from messytables import *


class DateParserTest(unittest.TestCase):
    def test_date_regex(self):
        assert dateparser.is_date('2012 12 22')
        assert dateparser.is_date('2012/12/22')
        assert dateparser.is_date('2012-12-22')
        assert dateparser.is_date('22.12.2012')
        assert dateparser.is_date('12 12 22')
        assert dateparser.is_date('22 Dec 2012')
        assert dateparser.is_date('2012 12 22 13:17')
        assert dateparser.is_date('2012 12 22 T 13:17')


class ReadTest(unittest.TestCase):
    def test_read_simple_csv(self):
        fh = horror_fobj('simple.csv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        assert_equal(7, len(list(row_set)))
        row = list(row_set.sample)[0]
        assert_equal(row[0].value, 'date')
        assert_equal(row[1].value, 'temperature')

        for row in list(row_set):
            assert_equal(3, len(row))
            assert_equal(row[0].type, StringType())

    def test_read_simple_zip(self):
        fh = horror_fobj('simple.zip')
        table_set = ZIPTableSet(fh)
        row_set = table_set.tables[0]
        assert_equal(7, len(list(row_set)))
        row = list(row_set.sample)[0]
        assert_equal(row[0].value, 'date')
        assert_equal(row[1].value, 'temperature')

        for row in list(row_set):
            assert_equal(3, len(row))
            assert_equal(row[0].type, StringType())

    def test_read_complex_csv(self):
        fh = horror_fobj('complex.csv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        assert_equal(4, len(list(row_set)))
        row = list(row_set.sample)[0]
        assert_equal(row[0].value, 'date')
        assert_equal(row[1].value, 'another date')
        assert_equal(row[2].value, 'temperature')
        assert_equal(row[3].value, 'place')

        for row in list(row_set):
            assert_equal(4, len(row))
            assert_equal(row[0].type, StringType())

    def test_read_simple_tsv(self):
        fh = horror_fobj('example.tsv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        assert_equal(141, len(list(row_set)))
        row = list(row_set.sample)[0]
        assert_equal(row[0].value, 'hour')
        assert_equal(row[1].value, 'expr1_0_imp')
        for row in list(row_set):
            assert_equal(17, len(row))
            assert_equal(row[0].type, StringType())

    def test_read_simple_ssv(self):
        # semicolon separated values
        fh = horror_fobj('simple.ssv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        assert_equal(7, len(list(row_set)))
        row = list(row_set.sample)[0]
        assert_equal(row[0].value, 'date')
        assert_equal(row[1].value, 'temperature')

        for row in list(row_set):
            assert_equal(3, len(row))
            assert_equal(row[0].type, StringType())

    def test_overriding_sniffed(self):
        # semicolon separated values
        fh = horror_fobj('simple.csv')
        table_set = CSVTableSet(fh, delimiter=";")
        row_set = table_set.tables[0]
        assert_equal(7, len(list(row_set)))
        row = list(row_set.sample)[0]
        assert_equal(len(row), 1)

    def test_read_simple_xls(self):
        fh = horror_fobj('simple.xls')
        table_set = XLSTableSet(fh)
        assert_equal(1, len(table_set.tables))
        row_set = table_set.tables[0]
        row = list(row_set.sample)[0]
        assert_equal(row[0].value, 'date')
        assert_equal(row[1].value, 'temperature')
        assert_equal(row[2].value, 'place')

        for row in list(row_set):
            assert 3 == len(row), row

    def test_read_simple_xlsx(self):
        fh = horror_fobj('simple.xlsx')
        table_set = XLSXTableSet(fh)
        assert_equal(1, len(table_set.tables))
        row_set = table_set.tables[0]
        row = list(row_set.sample)[0]
        assert_equal(row[0].value, 'date')
        assert_equal(row[1].value, 'temperature')
        assert_equal(row[2].value, 'place')

        for row in list(row_set):
            assert 3 == len(row), row

    def test_read_head_padding_csv(self):
        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        offset, headers = headers_guess(row_set.sample)
        assert 11 == len(headers), headers
        assert_equal(u'1985', headers[1].strip())
        row_set.register_processor(headers_processor(headers))
        row_set.register_processor(offset_processor(offset + 1))
        data = list(row_set.sample)
        for row in row_set:
            assert_equal(11, len(row))
        value = data[1][0].value.strip()
        assert value == u'Gefäßchirurgie', value

    def test_read_head_offset_csv(self):
        fh = horror_fobj('simple.csv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        offset, headers = headers_guess(row_set.sample)
        assert_equal(offset, 0)
        row_set.register_processor(offset_processor(offset + 1))
        data = list(row_set.sample)
        assert_equal(int(data[0][1].value), 1)
        data = list(row_set)
        assert_equal(int(data[0][1].value), 1)

    def test_read_head_offset_excel(self):
        fh = horror_fobj('simple.xls')
        table_set = XLSTableSet(fh)
        row_set = table_set.tables[0]
        offset, headers = headers_guess(row_set.sample)
        assert_equal(offset, 0)
        row_set.register_processor(offset_processor(offset + 1))
        data = list(row_set.sample)
        assert_equal(int(data[0][1].value), 1)
        data = list(row_set)
        assert_equal(int(data[0][1].value), 1)

    def test_guess_headers(self):
        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        offset, headers = headers_guess(row_set.sample)
        row_set.register_processor(headers_processor(headers))
        row_set.register_processor(offset_processor(offset + 1))
        data = list(row_set)
        assert 'Frauenheilkunde' in data[9][0].value, data[9][0].value

        fh = horror_fobj('weird_head_padding.csv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        row_set.register_processor(headers_processor(['foo', 'bar']))
        data = list(row_set)
        assert 'foo' in data[12][0].column, data[12][0]
        assert 'Chirurgie' in data[12][0].value, data[12][0].value

    def test_read_type_guess_simple(self):
        fh = horror_fobj('simple.csv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        types = type_guess(row_set.sample)
        expected_types = [DateType("%Y-%m-%d"), IntegerType(), StringType()]
        assert_equal(types, expected_types)

        row_set.register_processor(types_processor(types))
        data = list(row_set)
        header_types = map(lambda c: c.type, data[0])
        assert_equal(header_types, [StringType()] * 3)
        row_types = map(lambda c: c.type, data[2])
        assert_equal(expected_types, row_types)

    def test_read_type_know_simple(self):
        fh = horror_fobj('simple.xls')
        table_set = XLSTableSet(fh)
        row_set = table_set.tables[0]
        row = list(row_set.sample)[1]
        types = [c.type for c in row]
        assert_equal(types, [DateType(None), FloatType(), StringType()])

    def test_read_encoded_csv(self):
        fh = horror_fobj('utf-16le_encoded.csv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        assert_equal(328, len(list(row_set)))
        row = list(row_set.sample)[0]
        assert_equal(row[1].value, 'Organisation_name')

    def test_long_csv(self):
        fh = horror_fobj('long.csv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(4000, len(data))

    def test_small_csv(self):
        fh = horror_fobj('small.csv')
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(1, len(data))

    def test_skip_initials(self):
        def rows(skip_policy):
            fh = horror_fobj('skip_initials.csv')
            table_set = CSVTableSet(fh,
                                    skipinitialspace=skip_policy)
            row_set = table_set.tables[0]
            return row_set

        second = lambda r: r[1].value

        assert "goodbye" in map(second, rows(True))
        assert "    goodbye" in map(second, rows(False))

    def test_bad_first_sheet(self):
        # First sheet appears to have no cells
        fh = horror_fobj('problematic_first_sheet.xls')
        table_set = XLSTableSet(fh)
        tables = table_set.tables
        assert_equal(0, len(list(tables[0].sample)))
        assert_equal(1000, len(list(tables[1].sample)))

    def test_rowset_as_schema(self):
        from StringIO import StringIO as sio
        ts = CSVTableSet(sio('''name,dob\nmk,2012-01-02\n'''))
        rs = ts.tables[0]
        jts = rowset_as_jts(rs).as_dict()
        assert_equal(jts['fields'], [{'type': 'string', 'id': u'name', 'label': u'name'},
                                     {'type': 'date', 'id': u'dob', 'label': u'dob'}])


class TypeGuessTest(unittest.TestCase):
    def test_type_guess(self):
        csv_file = StringIO.StringIO('''
            1,   2012/2/12, 2,   02 October 2011
            2,   2012/2/12, 2,   02 October 2011
            2.4, 2012/2/12, 1,   1 May 2011
            foo, bar,       1000,
            4.3, ,          42,  24 October 2012
             ,   2012/2/12, 21,  24 December 2013''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows.sample)

        assert_equal(guessed_types, [DecimalType(), DateType('%Y/%m/%d'), IntegerType(), DateType('%d %B %Y')])

    def test_type_guess_strict(self):
        import locale
        locale.setlocale(locale.LC_ALL, 'en_GB.UTF-8')
        csv_file = StringIO.StringIO('''
            1,   2012/2/12, 2,      2,02 October 2011,"100.234354"
            2,   2012/2/12, 1.1,    0,1 May 2011,"100,000,000.12"
            foo, bar,       1500,   0,,"NaN"
            4,   2012/2/12, 42,"-2,000",24 October 2012,"42"
            ,,,,,''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows.sample, strict=True)
        assert_equal(guessed_types, [StringType(), StringType(), DecimalType(), IntegerType(), DateType('%d %B %Y'), FloatType()])

    def test_strict_guessing_handles_padding(self):
        csv_file = StringIO.StringIO('''
            1,   , 2
            2,   , 1.1
            foo, , 1500''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows.sample, strict=True)
        assert_equal(len(guessed_types), 3)
        assert_equal(guessed_types, [StringType(), StringType(), DecimalType()])

    def test_non_strict_guessing_handles_padding(self):
        csv_file = StringIO.StringIO('''
            1,   , 2
            2,   , 1.1
            foo, , 1500''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows.sample, strict=False)
        assert_equal(len(guessed_types), 3)
        assert_equal(guessed_types, [IntegerType(), StringType(), DecimalType()])


class TestStreamInput(unittest.TestCase):
    @httprettified
    def test_http_csv(self):
        url = 'http://www.messytables.org/static/long.csv'
        HTTPretty.register_uri(HTTPretty.GET, url,
            body=horror_fobj('long.csv').read(),
            content_type="application/csv")
        fh = urllib2.urlopen(url)
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(4000, len(data))

    @httprettified
    def test_http_csv_requests(self):
        url = 'http://www.messytables.org/static/long.csv'
        HTTPretty.register_uri(HTTPretty.GET, url,
            body=horror_fobj('long.csv').read(),
            content_type="application/csv")
        r = requests.get(url, stream=True)
        # no full support for non blocking version yet, use urllib2
        fh = StringIO.StringIO(r.raw.read())
        table_set = CSVTableSet(fh, encoding='utf-8')
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(4000, len(data))

    @httprettified
    def test_http_csv_encoding(self):
        url = 'http://www.messytables.org/static/utf-16le_encoded.csv'
        HTTPretty.register_uri(HTTPretty.GET, url,
            body=horror_fobj('utf-16le_encoded.csv').read(),
            content_type="application/csv")
        fh = urllib2.urlopen(url)
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(328, len(data))

    @httprettified
    def test_http_xls(self):
        url = 'http://www.messytables.org/static/simple.xls'
        HTTPretty.register_uri(HTTPretty.GET, url,
            body=horror_fobj('simple.xls').read(),
            content_type="application/ms-excel")
        fh = urllib2.urlopen(url)
        table_set = XLSTableSet(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(7, len(data))

    @httprettified
    def test_http_xlsx(self):
        url = 'http://www.messytables.org/static/simple.xlsx'
        HTTPretty.register_uri(HTTPretty.GET, url,
            body=horror_fobj('simple.xlsx').read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        fh = urllib2.urlopen(url)
        table_set = XLSXTableSet(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(7, len(data))


if __name__ == '__main__':
    unittest.main()
