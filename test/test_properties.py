# -*- coding: utf-8 -*-

import unittest
from . import horror_fobj
from messytables.any import any_tableset
from nose.tools import assert_true, assert_equal, assert_false, assert_raises
import lxml.html

try:
    # Python 2.6 doesn't provide assert_is_instance
    # TODO move out into separate module, used by test/test_read.py too
    from nose.tools import assert_is_instance
except ImportError:
    def assert_is_instance(obj, cls, msg=None):
        if not isinstance(obj, cls):
            raise AssertionError('Expected an instance of %r, got a %r' % (
                                 cls, obj.__class__))


class TestCellProperties(unittest.TestCase):
    def test_core_properties(self):
        csv = any_tableset(horror_fobj('simple.csv'), extension="csv")
        for table in csv.tables:
            for row in table:
                for cell in row:
                    cell.properties  # vague existence
                    assert_false('anything' in cell.properties)


class TestCoreProperties(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = any_tableset(horror_fobj('rowcolspan.html'),
                                extension="html")
        cls.first_row = list(list(cls.html.tables)[0])[0]
        cls.real_cell = cls.first_row[1]

    def test_properties_implements_in(self):
        assert_true('html' in self.real_cell.properties)
        assert_false('invalid' in self.real_cell.properties)

    def test_properties_implements_keys(self):
        assert_equal(['_lxml', 'html'], self.real_cell.properties.keys())

    def test_properties_implements_items(self):
        self.real_cell.properties.items()

    def test_properties_implements_get(self):
        assert_equal('default', self.real_cell.properties.get(
            'not_in_properties', 'default'))
        assert_equal(None, self.real_cell.properties.get('not_in_properties'))


class TestHtmlProperties(unittest.TestCase):
    # <td colspan='2'> would create one 'real' and one 'fake' cell
    @classmethod
    def setUpClass(cls):
        cls.html = any_tableset(horror_fobj('rowcolspan.html'),
                                extension="html")
        cls.first_row = list(list(cls.html.tables)[0])[0]
        cls.real_cell = cls.first_row[1]
        cls.fake_cell = cls.first_row[2]

    def test_real_cells_have_properties(self):
        assert_equal(
            set(['_lxml', 'html']),
            set(self.real_cell.properties.keys()))

    def test_real_cells_have_lxml_property(self):
        lxml_element = self.real_cell.properties['_lxml']
        assert_is_instance(lxml_element, lxml.html.HtmlElement)
        assert_equal('<td colspan="2">06</td>',
                     lxml.html.tostring(lxml_element))

    def test_fake_cells_have_no_lxml_property(self):
        assert_raises(KeyError, lambda: self.fake_cell.properties['_lxml'])

    def test_real_cells_have_html_property(self):
        html = self.real_cell.properties['html']
        assert_is_instance(html, basestring)
        assert_equal('<td colspan="2">06</td>', html)

    def test_fake_cells_have_no_html_property(self):
        assert_raises(KeyError, lambda: self.fake_cell.properties['html'])
