# -*- coding: utf-8 -*-

import unittest
from . import horror_fobj
from messytables.any import any_tableset
from messytables.html import HTMLProperties
from nose.tools import assert_true, assert_equal, assert_false, assert_raises
import lxml.html

try:
    # Python 2.6 doesn't provide assert_is_instance
    # TODO move out into separate module, used by test/test_read.py too
    from nose.tools import assert_is_instance
except ImportError:
    def assert_is_instance(obj, cls, msg=None):
        if not isinstance(obj, cls):
            raise AssertionError('Expected an instance of %r, got a %r' % (cls, obj.__class__))

csv = any_tableset(horror_fobj('simple.csv'), extension="csv")
html = any_tableset(horror_fobj('rowcolspan.html'), extension="html")
html_first_row = list(list(html.tables)[0])[0]
class TestCellProperties(unittest.TestCase):
    def test_core_properties(self):
        for table in csv.tables:
           for row in table:
              for cell in row:
                 cell.properties  # vague existence
                 assert_false('anything' in cell.properties)


class TestHtmlProperties(unittest.TestCase):
    def test_html_properties(self):
        for table in html.tables:
           for row in table:
              for cell in row:
                  assert_is_instance(cell.properties, HTMLProperties)
                  assert_is_instance(cell.properties['colspan'],int)

    def test_real_cells_have_lxml_property(self):
        real_cell = html_first_row[1]
        lxml_element = real_cell.properties['_lxml']
        assert_is_instance(lxml_element, lxml.html.HtmlElement)
        assert_equal('<td colspan=2>06</td>', lxml.html.tostring(lxml_element))

    def test_fake_cells_have_no_lxml_property(self):
        """<td colspan='2'> would create one 'real' and one 'fake' cell"""
        fake_cell = html_first_row[2]
        assert_raises(KeyError, lambda: fake_cell.properties['_lxml'])
               
    def test_real_cells_have_html_property(self):
        real_cell = html_first_row[1]
        html = real_cell.properties['html']
        assert_is_instance(html, basestring)
        assert_equal('<td colspan=2>06</td>', html)

    def test_fake_cells_have_no_html_property(self):
        """<td colspan='2'> would create one 'real' and one 'fake' cell"""
        fake_cell = html_first_row[2]
        assert_raises(KeyError, lambda: fake_cell.properties['html'])
