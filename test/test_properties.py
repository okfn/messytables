# -*- coding: utf-8 -*-

import unittest
from . import horror_fobj
from messytables.any import any_tableset
from messytables.error import NoSuchPropertyError
from nose.tools import (
    assert_equal,
    assert_false,
    assert_raises,
    assert_true)
import lxml.html

try:
    # Python 2.6 doesn't provide assert_is_instance
    from nose.tools import assert_is_instance, assert_greater_equal
except ImportError:
    from .shim26 import assert_is_instance, assert_greater_equal


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
        first_row = list(list(cls.html.tables)[0])[0]
        cls.real_cell = first_row[1]

    def test_properties_implements_in(self):
        assert_true('html' in self.real_cell.properties)
        assert_false('invalid' in self.real_cell.properties)

    def test_properties_implements_keys(self):
        assert(list(self.real_cell.properties.keys()))

    def test_properties_implements_items(self):
        assert(list(self.real_cell.properties.items()))

    def test_properties_implements_get(self):
        assert_equal('default', self.real_cell.properties.get(
            'not_in_properties', 'default'))
        assert_equal(None, self.real_cell.properties.get('not_in_properties'))


class TestExcelSpanRich(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.xls = any_tableset(horror_fobj('span_rich.xls'), extension='xls')
        cls.table = list(list(cls.xls.tables)[0])

    def test_basic_rich(self):
        assert self.table[4][1].properties['richtext']
        assert not list(self.table)[4][2].properties['richtext']
        assert self.table[4][1].value == 'bold and italic'

    def test_topleft(self):
        assert self.table[0][0].topleft, self.table[0][0].value
        assert self.table[1][1].topleft, self.table[1][1].value
        assert not self.table[1][2].topleft, self.table[1][2].value
        assert not self.table[2][2].topleft, self.table[2][2].value


class TestExcelProperties(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.xls = any_tableset(horror_fobj('excel_properties.xls'), extension='xls')
        rows = list(cls.xls.tables)[0]
        cls.first_cells = [list(row)[0] for row in rows]
        cls.properties = [x.properties for x in cls.first_cells]

    def test_cell_has_bold(self):
        assert_true('bold' in self.properties[0])
        assert_true(self.properties[0]['bold'])
        assert_false(self.properties[1]['bold'])

    def test_cell_has_italic(self):
        assert_true(self.properties[1]['italic'])
        assert_false(self.properties[0]['italic'])

    def test_cell_has_underline(self):
        assert_true(self.properties[2]['underline'])
        assert_false(self.properties[1]['underline'])

    def test_cell_size(self):
        assert_true(self.properties[9]['size'] > 20)
        assert_true(self.properties[10]['size'] < 8)
        assert_true(self.properties[0]['size'] == 10)

    def test_cell_has_borders(self):
        assert_false(self.properties[0]['any_border'])
        assert_false(self.properties[0]['all_border'])
        assert_true(self.properties[7]['any_border'])
        assert_false(self.properties[7]['all_border'])
        assert_true(self.properties[8]['any_border'])
        assert_true(self.properties[8]['all_border'])

    def test_cell_has_fontname(self):
        assert_true(self.properties[0]['font_name'] == 'Arial')

    def test_cell_has_strikeout(self):
        assert_true(self.properties[11]['strikeout'])
        assert_false(self.properties[0]['strikeout'])

    def test_blank_cells(self):
        assert_true(self.properties[12]['blank'])
        assert_false(self.properties[13]['blank'])

    def test_date(self):
        assert_false(self.properties[13]['a_date'])
        assert_true(self.properties[14]['a_date'])
        assert_equal(self.properties[14]['formatting_string'], r'DD/MM/YY')
        assert_equal(self.properties[15]['formatting_string'], r'QQ\ YY')
        assert_equal(self.properties[16]['formatting_string'], r'YYYY')
        assert_equal(self.properties[17]['formatting_string'], r'YYYY\ MMM')
        assert_equal(self.properties[18]['formatting_string'], r'D\ MMM\ YYYY')




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
        assert_greater_equal(
            set(self.real_cell.properties.keys()),
            set(['_lxml', 'html'])
            )

    def test_real_cells_have_lxml_property(self):
        lxml_element = self.real_cell.properties['_lxml']
        assert_is_instance(lxml_element, lxml.etree._Element)
        assert_equal(b'<td colspan="2">06</td>',
                     lxml.html.tostring(lxml_element))

    def test_real_cell_has_a_colspan(self):
        assert_equal(self.real_cell.properties['colspan'], 2)

    def test_fake_cells_have_no_lxml_property(self):
        assert_raises(KeyError, lambda: self.fake_cell.properties['_lxml'])
        assert_raises(NoSuchPropertyError, lambda: self.fake_cell.properties['_lxml'])

    def test_real_cells_have_html_property(self):
        html = self.real_cell.properties['html']
        assert_is_instance(html, bytes)
        assert_equal(b'<td colspan="2">06</td>', html)

    def test_fake_cells_have_no_html_property(self):
        assert_raises(KeyError, lambda: self.fake_cell.properties['html'])

class TestBrokenColspans(unittest.TestCase):
    def setUp(self):
        self.html = any_tableset(horror_fobj("badcolspan.html"),
                                 extension="html")

    def test_first_row(self):
        first_row = list(list(self.html.tables)[0])[0]
        self.assertEqual([cell.properties['colspan'] for cell in first_row],
                         [1, 1, 1, 1])

    def test_second_row(self):
        second_row = list(list(self.html.tables)[0])[1]
        self.assertEqual([cell.properties['colspan'] for cell in second_row],
                         [1, 1, 1, 1])

    def test_third_row(self):
        third_row = list(list(self.html.tables)[0])[2]
        self.assertEqual([cell.properties['colspan'] for cell in third_row],
                         [1, 1, 1, 1])

    def test_fourth_row(self):
        fourth_row = list(list(self.html.tables)[0])[3]
        self.assertEqual([cell.properties['colspan'] for cell in fourth_row],
                         [1, 1, 1, 1])
