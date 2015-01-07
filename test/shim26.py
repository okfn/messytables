def assert_is_instance(obj, cls, msg=None):
    if not isinstance(obj, cls):
        raise AssertionError('Expected an instance of %r, got a %r' % (
                             cls, obj.__class__))


def assert_greater_equal(obj, other, msg=None):
    if not obj > other:
        raise AssertionError('Expected {!r} > {!r}.'.format(obj, other))
