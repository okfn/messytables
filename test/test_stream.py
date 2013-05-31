# -*- coding: utf-8 -*-
import unittest
import urllib2
import requests
import StringIO

from . import horror_fobj
from nose.tools import assert_equal
import httpretty

from messytables import CSVTableSet, XLSTableSet, XLSXTableSet


class StreamInputTest(unittest.TestCase):
    @httpretty.activate
    def test_http_csv(self):
        url = 'http://www.messytables.org/static/long.csv'
        httpretty.register_uri(
            httpretty.GET, url,
            body=horror_fobj('long.csv').read(),
            content_type="application/csv")
        fh = urllib2.urlopen(url)
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
        fh = StringIO.StringIO(r.raw.read())
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
        fh = urllib2.urlopen(url)
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
        fh = urllib2.urlopen(url)
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
        fh = urllib2.urlopen(url)
        table_set = XLSXTableSet(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(7, len(data))
