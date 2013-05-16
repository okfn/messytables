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

    def test(self, value):
        """ Test if the value is of the given type. The
        default implementation calls ``cast`` and checks if
        that throws an exception. True or False"""
        if isinstance(value, self.result_type):
            return True
        try:
            self.cast(value)
            return True
        except:
            return False

    @classmethod
    def instances(cls):
        return [cls()]

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
        if value is None:
            return None
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
        if value in ['', None]:
            return None
        try:
            return int(value)
        except:
            return locale.atoi(value)


class FloatType(CellType):
    """ Floating point number. """
    guessing_weight = 4
    result_type = float

    def cast(self, value):
        if value in ['', None]:
            return None
        try:
            return float(value)
        except:
            return locale.atof(value)


class DecimalType(CellType):
    """ Decimal number, ``decimal.Decimal``. """
    guessing_weight = 5
    result_type = decimal.Decimal

    def cast(self, value):
        if value in ['', None]:
            return None
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
    def instances(cls):
        return [cls(v) for v in cls.formats]

    def test(self, value):
        if isinstance(value, basestring) and not is_date(value):
            return False
        return CellType.test(self, value)

    def cast(self, value):
        if isinstance(value, self.result_type):
            return value
        if value in ['', None]:
            return None
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
        if value in ['', None]:
            return None
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
    guesses = []
    type_instances = [i for t in types for i in t.instances()]
    if strict:
        at_least_one_value = []
        for ri, row in enumerate(rows):
            diff = len(row) - len(guesses)
            for _ in range(diff):
                typesdict = {}
                for type in type_instances:
                    typesdict[type] = 0
                guesses.append(typesdict)
                at_least_one_value.append(False)
            for ci, cell in enumerate(row):
                if not cell.value:
                    continue
                at_least_one_value[ci] = True
                for type in guesses[ci].keys():
                    if not type.test(cell.value):
                        guesses[ci].pop(type)
        # no need to set guessing weights before this
        for i, guess in enumerate(guesses):
            for type in guess.keys():
                guesses[i][type] = type.guessing_weight
        # in case there were no values at all in the column,
        # we just set the guessed type to string
        for i, v in enumerate(at_least_one_value):
            if not v:
                guesses[i] = {StringType(): 0}
    else:
        for i, row in enumerate(rows):
            diff = len(row) - len(guesses)
            for _ in range(diff):
                guesses.append(defaultdict(int))
            for i, cell in enumerate(row):
                # add string guess so that we have at least one guess
                guesses[i][StringType()] = 0
                if not cell.value:
                    continue
                for type in type_instances:
                    if type.test(cell.value):
                        guesses[i][type] += type.guessing_weight
        _columns = []
    _columns = []
    for types in guesses:
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
                if strict and type:
                    raise
        return row
    return apply_types
