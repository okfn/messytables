try:
    # python 2.7:
    from collections import OrderedDict
except ImportError:
    ## {{{ http://code.activestate.com/recipes/576669/ (r18)
    ## Raymond Hettingers proporsal to go in 2.7
    from collections import MutableMapping

    class OrderedDict(dict, MutableMapping):

        # Methods with direct access to underlying attributes

        def __init__(self, *args, **kwds):
            if len(args) > 1:
                raise TypeError('expected at 1 argument, got %d', len(args))
            if not hasattr(self, '_keys'):
                self._keys = []
            self.update(*args, **kwds)

        def clear(self):
            del self._keys[:]
            dict.clear(self)

        def __setitem__(self, key, value):
            if key not in self:
                self._keys.append(key)
            dict.__setitem__(self, key, value)

        def __delitem__(self, key):
            dict.__delitem__(self, key)
            self._keys.remove(key)

        def __iter__(self):
            return iter(self._keys)

        def __reversed__(self):
            return reversed(self._keys)

        def popitem(self):
            if not self:
                raise KeyError
            key = self._keys.pop()
            value = dict.pop(self, key)
            return key, value

        def __reduce__(self):
            items = [[k, self[k]] for k in self]
            inst_dict = vars(self).copy()
            inst_dict.pop('_keys', None)
            return (self.__class__, (items,), inst_dict)

        # Methods with indirect access via the above methods

        setdefault = MutableMapping.setdefault
        update = MutableMapping.update
        pop = MutableMapping.pop
        keys = MutableMapping.keys
        values = MutableMapping.values
        items = MutableMapping.items

        def __repr__(self):
            pairs = ', '.join(map('%r: %r'.__mod__, self.items()))
            return '%s({%s})' % (self.__class__.__name__, pairs)

        def copy(self):
            return self.__class__(self)

        @classmethod
        def fromkeys(cls, iterable, value=None):
            d = cls()
            for key in iterable:
                d[key] = value
            return d
    ## end of http://code.activestate.com/recipes/576669/ }}}


def offset_processor(offset):
    """ Skip ``offset`` from the given iterator. This can
    be used in combination with the ``headers_processor`` to
    apply the result of a header scan to the table. """
    def apply_offset(row_set, row):
        if not hasattr(row_set, '_offset'):
            row_set._offset = 0
        if row_set._offset >= offset:
            return row
        row_set._offset += 1
    return apply_offset
