
class MessytablesError(Exception):
    """A generic error to inherit from."""


class ReadError(MessytablesError):
    """Error reading the file/stream in terms of the expected format."""


class TableError(MessytablesError, LookupError):
    """Couldn't identify correct table."""


class NoSuchPropertyError(MessytablesError, KeyError):
    """The requested property doesn't exist."""
