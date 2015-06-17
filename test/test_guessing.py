# -*- coding: utf-8 -*-
import unittest
import io

from . import horror_fobj
from nose.plugins.attrib import attr
from nose.tools import assert_equal
from messytables import (CSVTableSet, type_guess, headers_guess,
                         offset_processor, DateType, StringType,
                         DecimalType, IntegerType,
                         DateUtilType, BoolType)


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
            DecimalType(), DateType('%Y/%m/%d'), IntegerType(),
            DateType('%d %B %Y'), BoolType(), BoolType()])

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
            StringType(), StringType(),
            DecimalType(), IntegerType(), DateType('%d %B %Y'),
            DecimalType()])

    def test_strict_guessing_handles_padding(self):
        csv_file = io.BytesIO(b'''
            1,   , 2
            2,   , 1.1
            foo, , 1500''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows.sample, strict=True)
        assert_equal(len(guessed_types), 3)
        assert_equal(guessed_types,
                     [StringType(), StringType(), DecimalType()])

    def test_non_strict_guessing_handles_padding(self):
        csv_file = io.BytesIO(b'''
            1,   , 2.1
            2,   , 1.1
            foo, , 1500''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(rows.sample, strict=False)
        assert_equal(len(guessed_types), 3)
        assert_equal(guessed_types,
                     [IntegerType(), StringType(), DecimalType()])

    def test_guessing_uses_first_in_case_of_tie(self):
        csv_file = io.BytesIO(b'''
            2
            1.1
            1500''')
        rows = CSVTableSet(csv_file).tables[0]
        guessed_types = type_guess(
            rows.sample, types=[DecimalType, IntegerType], strict=False)
        assert_equal(guessed_types, [DecimalType()])

        guessed_types = type_guess(
            rows.sample, types=[IntegerType, DecimalType], strict=False)
        assert_equal(guessed_types, [IntegerType()])

    @attr("slow")
    def test_strict_type_guessing_with_large_file(self):
        fh = horror_fobj('211.csv')
        rows = CSVTableSet(fh).tables[0]
        offset, headers = headers_guess(rows.sample)
        rows.register_processor(offset_processor(offset + 1))
        types = [StringType, IntegerType, DecimalType, DateUtilType]
        guessed_types = type_guess(rows.sample, types, True)
        assert_equal(len(guessed_types), 96)
        assert_equal(guessed_types, [
            IntegerType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            IntegerType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), IntegerType(), StringType(), DecimalType(),
            DecimalType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            IntegerType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            IntegerType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), DateUtilType(),
            DateUtilType(), DateUtilType(), DateUtilType(), StringType(),
            StringType(), StringType()])

    def test_file_with_few_strings_among_integers(self):
        fh = horror_fobj('mixedGLB.csv')
        rows = CSVTableSet(fh).tables[0]
        offset, headers = headers_guess(rows.sample)
        rows.register_processor(offset_processor(offset + 1))
        types = [StringType, IntegerType, DecimalType, DateUtilType]
        guessed_types = type_guess(rows.sample, types, True)
        assert_equal(len(guessed_types), 19)
        print(guessed_types)
        assert_equal(guessed_types, [
            IntegerType(), IntegerType(),
            IntegerType(), IntegerType(), IntegerType(), IntegerType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), StringType(), StringType(),
            StringType(), StringType(), IntegerType(), StringType(),
            StringType()])

    def test_integer_and_float_detection(self):
        def helper(value):
            return any(i.test(value) for i in IntegerType.instances())

        assert_equal(helper(123), True)
        assert_equal(helper('123'), True)
        assert_equal(helper(123.0), True)
        assert_equal(helper('123.0'), True)
        assert_equal(helper(123.1), False)
        assert_equal(helper('123.1'), False)
