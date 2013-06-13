# -*- coding: utf-8 -*-

import unittest
from . import horror_fobj
from messytables.any import any_tableset


class TestRowSet(unittest.TestCase):
    def test_repr_ascii_not_unicode(self):
        """
        __repr__ must return a str (not unicode), see object.__repr__(self) in
        http://docs.python.org/2/reference/datamodel.html
        """
        fh = horror_fobj('unicode_sheet_name.xls')
        table_set = any_tableset(fh, extension='xls')

        x = repr(table_set.tables)
        self.assertTrue(isinstance(x, str))
