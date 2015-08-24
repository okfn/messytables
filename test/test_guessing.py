# -*- coding: utf-8 -*-
import unittest
import io

from . import horror_fobj
from nose.plugins.attrib import attr
from nose.tools import assert_equal
from typecast import Date, String, Decimal, Integer, Boolean
from messytables import (CSVTableSet, type_guess, headers_guess,
                         offset_processor)


class TypeGuessTest(unittest.TestCase):
    @attr("slow")
    def test_type_guess(self):
        csv_file = io.BytesIO(b'''
            1,   2012/2/12, 2,   02 October 2011,  yes,   1
            2,   2012/2/12, 2,   02 October 2011,  true,  1
            2.4, 2012/2/12, 1,   1 May 2011,       no,    0
            foo, bar,       1000, ,                false, 0
            4.3, ,          42,  24 October 2012,,
             ,   2012/2/12, 21,  24 December 2013, true,  1''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows.sample)

        assert_equal(guessed_types, [
            Decimal(), Date('%Y/%m/%d'), Integer(),
            Date('%d %b %Y'), Boolean(), Integer()])

    def test_type_guess_strict(self):
        import locale
        locale.setlocale(locale.LC_ALL, 'en_GB.UTF-8')
        csv_file = io.BytesIO(b'''
            1,   2012/2/12, 2,      2,02 October 2011,"100.234354"
            2,   2012/2/12, 1.1,    0,1 May 2011,"100,000,000.12"
            foo, bar,       1500,   0,,"NaN"
            4,   2012/2/12, 42,"-2,000",24 October 2012,"42"
            ,,,,,''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows.sample, strict=True)
        assert_equal(guessed_types, [
            String(), String(),
            Decimal(), Integer(), Date('%d %b %Y'),
            Decimal()])

    def test_strict_guessing_handles_padding(self):
        csv_file = io.BytesIO(b'''
            1,   , 2
            2,   , 1.1
            foo, , 1500''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows.sample, strict=True)
        assert_equal(len(guessed_types), 3)
        assert_equal(guessed_types,
                     [String(), String(), Decimal()])

    def test_non_strict_guessing_handles_padding(self):
        csv_file = io.BytesIO(b'''
            1,   , 2.1
            2,   , 1.1
            foo, , 1500''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows.sample, strict=False)
        assert_equal(len(guessed_types), 3)
        assert_equal(guessed_types,
                     [Integer(), String(), Decimal()])

    def test_guessing_uses_first_in_case_of_tie(self):
        csv_file = io.BytesIO(b'''
            2
            1.1
            2.1
            1500''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(
            rows.sample, types=[Decimal, Integer], strict=False)
        assert_equal(guessed_types, [Decimal()])

        guessed_types = type_guess(
            rows.sample, types=[Integer, Decimal], strict=False)
        assert_equal(guessed_types, [Integer()])

    @attr("slow")
    def test_strict_type_guessing_with_large_file(self):
        fh = horror_fobj('211.csv')
        rows = CSVTableSet(fh).tables[0]
        offset, headers = headers_guess(rows.sample)
        rows.register_processor(offset_processor(offset + 1))
        types = [String, Integer, Decimal, Date]
        guessed_types = type_guess(rows.sample, types, False)
        assert_equal(len(guessed_types), 96)
        print guessed_types
        assert_equal(guessed_types, [
            Integer(), String(), String(), String(),
            String(), String(), Integer(), String(), String(), String(),
            String(), String(), String(), Integer(), String(), String(),
            String(), String(), String(), String(), Integer(), String(),
            String(), String(), String(), String(), String(), String(),
            String(), Decimal(), Decimal(), String(), String(), String(),
            String(), String(), String(), String(), String(), String(),
            String(), String(), String(), String(), String(), String(),
            String(), String(), String(), String(), String(), String(),
            String(), String(), Integer(), String(), String(), String(),
            String(), String(), String(), String(), String(), String(),
            String(), String(), String(), String(), String(), String(),
            String(), String(), String(), String(), String(), String(),
            String(), String(), String(), String(), String(), String(),
            String(), String(), String(), String(), String(), String(),
            String(), String(), String(), Date('%d/%m/%y'), Date('%d/%m/%y'),
            String(), String(), String()])

    def test_file_with_few_strings_among_integers(self):
        fh = horror_fobj('mixedGLB.csv')
        rows = CSVTableSet(fh).tables[0]
        offset, headers = headers_guess(rows.sample)
        rows.register_processor(offset_processor(offset + 1))
        types = [String, Integer, Decimal, Date]
        guessed_types = type_guess(rows.sample, types, True)
        assert_equal(len(guessed_types), 19)
        print(guessed_types)
        assert_equal(guessed_types, [
            Integer(), Integer(),
            Integer(), Integer(), Integer(), Integer(),
            String(), String(), String(), String(),
            String(), String(), String(), String(),
            String(), String(), Integer(), String(),
            String()])

    def test_integer_and_float_detection(self):
        def helper(value):
            return any(i.test(value) == 1 for i in Integer.instances())

        assert_equal(helper(123), True)
        assert_equal(helper('123'), True)
        assert_equal(helper(123.0), True)
        assert_equal(helper('123.0'), False)
        assert_equal(helper(123.1), False)
        assert_equal(helper('123.1'), False)
