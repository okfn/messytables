# -*- coding: utf-8 -*-
import unittest

from . import horror_fobj
from nose.plugins.attrib import attr
from nose.tools import (assert_equal, assert_greater_equal, assert_true,
                        assert_raises)
try:
    # Python 2.6 doesn't provide assert_is_instance
    from nose.tools import assert_is_instance
except ImportError:
    def assert_is_instance(obj, cls, msg=None):
        assert_true(isinstance(obj, cls))

from messytables import (CSVTableSet, StringType, HTMLTableSet,
                         ZIPTableSet, XLSTableSet, XLSXTableSet, PDFTableSet,
                         ODSTableSet, headers_guess, headers_processor,
                         offset_processor, DateType, FloatType,
                         IntegerType, rowset_as_jts,
                         types_processor, type_guess, ReadError)
import datetime

class ReadInvisibleTextHtmlTest(unittest.TestCase):
    def test_invisible_text_html(self):
        fh = horror_fobj('invisible_text.html')
        table_set = HTMLTableSet(fh)
        row_set = table_set.tables[0]
        assert_equal(4, len(list(row_set)))
        row = list(row_set.sample)[1]
        assert_equal(row[5].value.strip(), '1 July 1879')
        