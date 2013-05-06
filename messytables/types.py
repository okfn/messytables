import decimal
import datetime
from collections import defaultdict
from itertools import izip_longest
import locale

import dateutil.parser as parser

from messytables.dateparser import DATE_FORMATS, is_date


class CellType(object):
    """ A cell type maintains information about the format
    of the cell, providing methods to check if a type is
    applicable to a given value and to convert a value to the
    type. """

    guessing_weight = 1
    # the type that the result will have
    result_type = None

    @classmethod
    def test(cls, value):
        """ Test if the value is of the given type. The
        default implementation calls ``cast`` and checks if
        that throws an exception. Returns the casted value or None"""
        if isinstance(value, cls.result_type):
            return cls()
        try:
            ins = cls()
            ins.cast(value)
            return ins
        except:
            return None

    def cast(self, value):
        """ Convert the value to the type. This may throw
        a quasi-random exception if conversion fails. """
        return value

    def __eq__(self, other):
        return self.__class__ == other.__class__

    def __hash__(self):
        return hash(self.__class__)

    def __repr__(self):
        return self.__class__.__name__.rsplit('Type', 1)[0]


class StringType(CellType):
    """ A string or other unconverted type. """
    result_type = basestring

    def cast(self, value):
        if isinstance(value, self.result_type):
            return value
        try:
            return unicode(value)
        except UnicodeEncodeError:
            return str(value)


class IntegerType(CellType):
    """ An integer field. """
    guessing_weight = 6
    result_type = int

    def cast(self, value):
        try:
            return int(value)
        except:
            return locale.atoi(value)


class FloatType(CellType):
    """ Floating point number. """
    guessing_weight = 4
    result_type = float

    def cast(self, value):
        return locale.atof(value)


class DecimalType(CellType):
    """ Decimal number, ``decimal.Decimal``. """
    guessing_weight = 5
    result_type = decimal.Decimal

    def cast(self, value):
        return decimal.Decimal(value)


class DateType(CellType):
    """ The date type is special in that it also includes a specific
    date format that is used to parse the date, additionally to the
    basic type information. """
    guessing_weight = 3
    formats = DATE_FORMATS
    result_type = datetime.datetime

    def __init__(self, format):
        self.format = format

    @classmethod
    def test(cls, value):
        if isinstance(value, basestring) and not is_date(value):
            return
        for v in cls.formats:
            ins = cls(v)
            try:
                ins.cast(value)
                return ins
            except:
                pass

    def cast(self, value):
        if isinstance(value, self.result_type):
            return value
        if self.format is None:
            return value
        return datetime.datetime.strptime(value, self.format)

    def __eq__(self, other):
        return isinstance(other, DateType) and \
                self.format == other.format

    def __repr__(self):
        return "Date(%s)" % self.format

    def __hash__(self):
        return hash(self.__class__) + hash(self.format)


class DateUtilType(CellType):
    """ The date util type uses the dateutil library to
    parse the dates. The advantage of this type over
    DateType is the speed and better date detection. However,
    it does not offer format detection.

    Do not use this together with the DateType"""
    guessing_weight = 3
    result_type = datetime.datetime

    def cast(self, value):
        return parser.parse(value)


TYPES = [StringType, IntegerType, FloatType, DecimalType, DateType]


def type_guess(rows, types=TYPES, strict=False):
    """ The type guesser aggregates the number of successful
    conversions of each column to each type, weights them by a
    fixed type priority and select the most probable type for
    each column based on that figure. It returns a list of
    ``CellType``. Empty cells are ignored.

    Strict means that a type will not be guessed
    if parsing fails for a single cell in the column."""
    guesses = defaultdict(lambda: defaultdict(int))
    for row in rows:
        for i, cell in enumerate(row):
            # add string guess so that we have at least one guess
            guesses[i][StringType()] = 0
            for type in types:
                if not cell.value:
                    continue
                guess = type.test(cell.value)
                if guess is None:
                    if strict:
                        for key in guesses[i].keys():
                            if isinstance(key, type):
                                guesses[i][key] = None
                else:
                    if guesses[i][guess] is None:
                        continue
                    guesses[i][guess] += type.guessing_weight
    _columns = []
    for i, types in guesses.items():
        _columns.append(max(types.items(), key=lambda (t, n): n)[0])
    return _columns


def types_processor(types, strict=False):
    """ Apply the column types set on the instance to the
    current row, attempting to cast each cell to the specified
    type.

    Strict means that casting errors are not ignored"""
    def apply_types(row_set, row):
        if types is None:
            return row
        for cell, type in izip_longest(row, types):
            try:
                cell.value = type.cast(cell.value)
                cell.type = type
            except:
                if strict and type and cell.value:
                    raise
        return row
    return apply_types
