
def skip_n(iterable, offset):
    """ Skip ``offset`` from ``iterable``. """
    zip(xrange(offset), iterable)
    for item in iterable:
        yield item
