# -*- coding: utf-8 -*-
import unittest

from . import horror_fobj
from nose.tools import assert_equal
from messytables import (any_tableset, XLSTableSet, ZIPTableSet,
                         CSVTableSet, XLSXTableSet)


class TestAny(unittest.TestCase):
    def test_simple_csv(self):
        fh = horror_fobj('simple.csv')
        table_set = any_tableset(fh, extension='csv')
        assert isinstance(table_set, CSVTableSet)

    def test_simple_xls(self):
        fh = horror_fobj('simple.xls')
        table_set = any_tableset(fh, extension='xls')
        assert isinstance(table_set, XLSTableSet)

    def test_simple_xlsx(self):
        fh = horror_fobj('simple.xlsx')
        table_set = any_tableset(fh, extension='xlsx')
        assert isinstance(table_set, XLSXTableSet)

    def test_simple_zip(self):
        fh = horror_fobj('simple.zip')
        table_set = any_tableset(fh, extension='zip')
        assert isinstance(table_set, ZIPTableSet)

    def test_xlsm(self):
        fh = horror_fobj('bian-anal-mca-2005-dols-eng-1011-0312-tab3.xlsm')
        table_set = any_tableset(fh, extension='xls')
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(62, len(data))
