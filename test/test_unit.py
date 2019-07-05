# -*- coding: utf-8 -*-
import unittest

from messytables import Cell


class CellReprTest(unittest.TestCase):
    def test_repr_ok(self):
        repr(Cell(value=u"\xa0"))
