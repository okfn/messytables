import decimal
from datetime import datetime
from pprint import pprint

from messytables.dateparser import DATE_FORMATS

class CellType(object):
    guessing_weight = 1

    @classmethod
    def test(cls, value):
        try:
            ins = cls()
            ins.cast(value)
            return ins
        except: 
            return None

    def cast(self, value):
        return value

    def __eq__(self, other):
        return self.__class__ == other.__class__

    def __hash__(self):
        return hash(self.__class__)

    def __repr__(self):
        return self.__class__.__name__.rsplit('Type', 1)[0]

class StringType(CellType):

    def cast(self, value):
        if not isinstance(value, basestring):
            raise ValueError()
        return value

class IntegerType(CellType):
    guessing_weight = 1.5

    def cast(self, value):
        return int(value)

class FloatType(CellType):
    guessing_weight = 2

    def cast(self, value):
        return float(value)

class DecimalType(CellType):
    guessing_weight = 2.5

    def cast(self, value):
        return decimal.Decimal(value)

class DateType(CellType):
    guessing_weight = 3

    def __init__(self, format):
        self.format = format

    @classmethod
    def test(cls, value):
        for k, v in DATE_FORMATS.items():
            ins = cls(v)
            try:
                ins.cast(value)
                return ins
            except: pass

    def cast(self, value):
        if self.format is None:
            return value
        return datetime.strptime(value, self.format)

    def __eq__(self, other):
        return isinstance(other, DateType) and \
                self.format == other.format
    
    def __repr__(self):
        return "Date(%s)" % self.format

    def __hash__(self):
        return hash(self.__class__) + hash(self.format)

TYPES = [StringType, IntegerType, FloatType, DecimalType, DateType]

from collections import defaultdict
def type_guess(rows):
    guesses = defaultdict(lambda: defaultdict(int))
    for row in rows:
        for i, cell in enumerate(row):
            for type in TYPES:
                guess = type.test(cell.value)
                if guess is not None:
                    guesses[i][guess] += 1
    for i, types in guesses.items():
        for type, count in types.items():
            guesses[i][type] *= type.guessing_weight
    _columns = []
    for i, types in guesses.items():
        _columns.append(max(types.items(), key=lambda (t, n): n)[0])
    return _columns

from itertools import izip_longest
def types_processor(types):
    """ Apply the column types set on the instance to the
    current row, attempting to cast each cell to the specified
    type. """
    def apply_types(row_set, row):
        if types is None:
            return row
        for cell, type in izip_longest(row, types):
            try:
                cell.value = type.cast(cell.value)
                cell.type = type
            except:
                pass
        return row
    return apply_types
