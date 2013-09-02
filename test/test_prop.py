# -*- coding: utf-8 -*-

import unittest
from . import horror_fobj
from messytables.any import any_tableset
from messytables.html import HTMLProperties
from nose.tools import assert_true, assert_equal, assert_false

csv = any_tableset(horror_fobj('simple.csv'), extension="csv")
html = any_tableset(horror_fobj('rowcolspan.html'), extension="html")
class TestCellProperties(unittest.TestCase):
    def test_core_properties(self):
        for table in csv.tables:
           for row in table:
              for cell in row:
                 cell.properties  # vague existence
                 assert_false('anything' in cell.properties)

    def test_html_properties(self):
        for table in html.tables:
           for row in table:
              for cell in row:
                  assert_true(isinstance(cell.properties, HTMLProperties))
                  assert_equal(type(cell.properties['colspan']),int)
          
