# -*- coding: utf-8 -*-
import unittest
from messytables.compat23 import urlopen
import requests
import io

from . import horror_fobj
from nose.tools import assert_equal
import httpretty

from messytables import CSVTableSet, XLSTableSet

class StreamInputTest(unittest.TestCase):
    @httpretty.activate
    def test_http_csv(self):
        url = 'http://www.messytables.org/static/long.csv'
        httpretty.register_uri(
            httpretty.GET, url,
            body=horror_fobj('long.csv').read(),
            content_type="application/csv")
        fh = urlopen(url)
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(4000, len(data))

    @httpretty.activate
    def test_http_csv_requests(self):
        url = 'http://www.messytables.org/static/long.csv'
        httpretty.register_uri(
            httpretty.GET, url,
            body=horror_fobj('long.csv').read(),
            content_type="application/csv")
        r = requests.get(url, stream=True)
        # no full support for non blocking version yet, use urllib2
        fh = io.BytesIO(r.raw.read())
        table_set = CSVTableSet(fh, encoding='utf-8')
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(4000, len(data))

    @httpretty.activate
    def test_http_csv_encoding(self):
        url = 'http://www.messytables.org/static/utf-16le_encoded.csv'
        httpretty.register_uri(
            httpretty.GET, url,
            body=horror_fobj('utf-16le_encoded.csv').read(),
            content_type="application/csv")
        fh = urlopen(url)
        table_set = CSVTableSet(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(328, len(data))

    @httpretty.activate
    def test_http_xls(self):
        url = 'http://www.messytables.org/static/simple.xls'
        httpretty.register_uri(
            httpretty.GET, url,
            body=horror_fobj('simple.xls').read(),
            content_type="application/ms-excel")
        fh = urlopen(url)
        table_set = XLSTableSet(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(7, len(data))

    @httpretty.activate
    def test_http_xlsx(self):
        url = 'http://www.messytables.org/static/simple.xlsx'
        httpretty.register_uri(
            httpretty.GET, url,
            body=horror_fobj('simple.xlsx').read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        fh = urlopen(url)
        table_set = XLSTableSet(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(7, len(data))
