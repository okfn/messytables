from datetime import datetime
import decimal 

STRING = unicode
INTEGER = int
FLOAT = float
DECIMAL = decimal.Decimal
DATE = datetime



def skip_n(iterable, offset):
    """ Skip ``offset`` from ``iterable``. """
    zip(xrange(offset), iterable)
    for item in iterable:
        yield item
