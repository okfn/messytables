#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from . import horror_fobj
from messytables.any import any_tableset
from messytables.core import RowSet
from messytables.error import TableError
try:
    from nose.tools import assert_is_instance
except ImportError:
    from shim26 import assert_is_instance


class TestTableSet(unittest.TestCase):
    def setUp(self):
        fh = horror_fobj('simple.xls')
        self.table_set = any_tableset(fh, extension='xls')

    def test_get_item(self):
        assert_is_instance(self.table_set['simple.csv'], RowSet)

    def test_missing_sheet(self):
        self.assertRaises(TableError, lambda: self.table_set['non-existent'])

        # TODO: It would be good if we could manipulate a tableset to have
        # multiple row sets of the same name, then enable the following test.

        # self.assertRaises(Error, lambda: table_set['duplicated-name'])
