#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import unittest
from nose.tools import assert_equal
from messytables import *


def horror_fobj(name):
    fn = os.path.join(os.path.dirname(__file__), '..', 'horror', name)
    return open(fn, 'rb')


class ReadTest(unittest.TestCase):
    def test_read_html(self):
        fh = horror_fobj('html.html')
        table_set = HTMLTableSet(fh)
        row_set = table_set.tables[0]
        assert_equal(200, len(list(row_set)))
        row = list(row_set.sample)[0]
        assert_equal(row[0].value.strip(), 'HDI Rank')
        assert_equal(row[1].value.strip(), 'Country')
        assert_equal(row[4].value.strip(), '2010')

    def test_read_span_html(self):
        fh = horror_fobj('rowcolspan.html')
        table_set = HTMLTableSet(fh)
        row_set = table_set.tables[0]

        magic = {}
        for y, row in enumerate(row_set):
            for x, cell in enumerate(row):
                magic[(x, y)] = cell.value

        tests = {(0, 0): '05',
                 (0, 2): '25',
                 (0, 3): '',
                 (1, 3): '36',
                 (1, 6): '66',
                 (4, 7): '79',
                 (4, 8): '89'}

        for test in tests:
            assert_equal(magic[test], tests[test])
