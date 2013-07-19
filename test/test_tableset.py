#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
from . import horror_fobj
from messytables.any import any_tableset
from messytables.core import RowSet


class TestTableSet(unittest.TestCase):
    def test_get_item(self):
        fh = horror_fobj('simple.xls')
        table_set = any_tableset(fh, extension='xls')

        self.assertTrue(isinstance(table_set['simple.csv'], RowSet))
        self.assertRaises(KeyError, lambda: table_set['non-existent'])

        # TODO: It would be good if we could manipulate a tableset to have
        # multiple row sets of the same name, then enable the following test.

        # self.assertRaises(Error, lambda: table_set['duplicated-name'])
