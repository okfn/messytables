# -*- coding: utf-8 -*-
import unittest

from . import horror_fobj
from nose.tools import assert_equal
from nose.plugins.skip import SkipTest
from messytables import (any_tableset, XLSTableSet, ZIPTableSet, PDFTableSet,
                         CSVTableSet, ODSTableSet,
                         ReadError)

suite = [{'filename': 'simple.csv', 'tableset': CSVTableSet},
         {'filename': 'simple.xls', 'tableset': XLSTableSet},
         {'filename': 'simple.xlsx', 'tableset': XLSTableSet},
         {'filename': 'simple.zip', 'tableset': ZIPTableSet},
         {'filename': 'simple.ods', 'tableset': ODSTableSet},
         {'filename': 'bian-anal-mca-2005-dols-eng-1011-0312-tab3.xlsm',
          'tableset': XLSTableSet},
         ]

# Special handling for PDFTables - skip if not installed
try:
    import pdftables
except ImportError:
    got_pdftables = False
    suite.append({"filename": "simple.pdf", "tableset": False})
else:
    from messytables import PDFTableSet
    got_pdftables = True
    suite.append({"filename": "simple.pdf", "tableset": PDFTableSet})


def test_simple():
    for d in suite:
        yield check_no_filename, d
        yield check_filename, d


def check_no_filename(d):
    if not d['tableset']:
        raise SkipTest("Optional library not installed. Skipping")
    fh = horror_fobj(d['filename'])
    table_set = any_tableset(fh)
    assert isinstance(table_set, d['tableset']), type(table_set)


def check_filename(d):
    if not d['tableset']:
        raise SkipTest("Optional library not installed. Skipping")
    fh = horror_fobj(d['filename'])
    table_set = any_tableset(fh, extension=d['filename'], auto_detect=False)
    assert isinstance(table_set, d['tableset']), type(table_set)


class TestAny(unittest.TestCase):
    def test_xlsm(self):
        fh = horror_fobj('bian-anal-mca-2005-dols-eng-1011-0312-tab3.xlsm')
        table_set = any_tableset(fh, extension='xls')
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(62, len(data))

    def test_unknown(self):
        fh = horror_fobj('simple.unknown')
        self.assertRaises(ReadError,
                          lambda: any_tableset(fh, extension='unknown'))

    def test_scraperwiki_xlsx(self):
        fh = horror_fobj('sw_gen.xlsx')
        table_set = any_tableset(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(16, len(data))

    def test_libreoffice_xlsx(self):
        fh = horror_fobj('libreoffice.xlsx')
        table_set = any_tableset(fh)
        row_set = table_set.tables[0]
        data = list(row_set)
        assert_equal(0, len(data))
