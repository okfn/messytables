class MessytablesError(Exception):
    """A generic error to inherit from"""


class ReadError(MessytablesError):
    '''Error reading the file/stream in terms of the expected format.'''
    pass


class TableError(MessytablesError, LookupError):
    """Couldn't identify correct table."""
    pass

class NoSuchPropertyError(MessytablesError, KeyError):
    """The requested property doesn't exist"""
    pass
