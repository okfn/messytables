import decimal
from datetime import datetime
from pprint import pprint

from messytables.util import DATE, \
        STRING, INTEGER, FLOAT, DECIMAL
from messytables.dateparser import DATE_FORMATS

WEIGHTS = {
    STRING: 1.0, 
    INTEGER: 1.5, 
    FLOAT: 2, 
    DECIMAL: 2.5,
    DATE: 3.0
    }

def is_string(val):
    return True

def is_int(val):
    try:
        int(val)
        return True
    except:
        return False

def is_float(val):
    try:
        float(val)
        return True
    except:
        return False

def is_decimal(val):
    try:
        decimal.Decimal(val)
        return True
    except:
        return False

def is_date(val):
    for k, v in DATE_FORMATS.items():
        try:
            datetime.strptime(val, v)
            return True
        except: pass


CHECKS = {
    STRING: is_string, 
    INTEGER: is_int, 
    FLOAT: is_float,
    DECIMAL: is_decimal,
    DATE: is_date
    }

from collections import defaultdict
def type_guess(rows):
    guesses = defaultdict(lambda: defaultdict(int))
    for row in rows:
        for i, cell in enumerate(row):
            for type, test in CHECKS.items():
                if test(cell.value):
                    guesses[i][type] += 1
    for i, types in guesses.items():
        for type, count in types.items():
            guesses[i][type] *= WEIGHTS[type]
    _columns = []
    for i, types in guesses.items():
        _columns.append(max(types.items(), key=lambda (t, n): n)[0])
    return _columns


