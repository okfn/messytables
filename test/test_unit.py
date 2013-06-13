# -*- coding: utf-8 -*-
import unittest

from messytables import dateparser


class DateParserTest(unittest.TestCase):
    def test_date_regex(self):
        assert dateparser.is_date('2012 12 22')
        assert dateparser.is_date('2012/12/22')
        assert dateparser.is_date('2012-12-22')
        assert dateparser.is_date('22.12.2012')
        assert dateparser.is_date('12 12 22')
        assert dateparser.is_date('22 Dec 2012')
        assert dateparser.is_date('2012 12 22 13:17')
        assert dateparser.is_date('2012 12 22 T 13:17')
