from collections import defaultdict

import six

from typecast import String, Integer, Decimal, Boolean, Date, DateTime

WEIGHTS = {
    String: 1,
    Integer: 6,
    Decimal: 3,
    Boolean: 7,
    Date: 4,
    DateTime: 5
}
TYPES = [String, Decimal, Integer, Boolean, Date, DateTime]
FAILED = 'failed'


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
    for i, row in enumerate(rows):
        diff = len(row) - len(guesses)
        for _ in range(diff):
            guesses.append(defaultdict(int))
        for j, cell in enumerate(row):
            # add string guess so that we have at least one guess
            guesses[j][String()] = guesses[j].get(String(), 0)
            for type in type_instances:
                if guesses[j][type] == FAILED:
                    continue
                result = type.test(cell.value)
                weight = WEIGHTS[type.__class__]
                if strict and (result == -1) and not isinstance(type, String):
                    guesses[j][type] = FAILED
                elif result == 1:
                    guesses[j][type] += weight

    _columns = []
    for guess in guesses:
        # this first creates an array of tuples because we want the types to be
        # sorted. Even though it is not specified, python chooses the first
        # element in case of a tie
        # See: http://stackoverflow.com/a/6783101/214950
        guesses_tuples = [(t, guess[t]) for t in type_instances
                          if t in guess and guess[t] != FAILED]
        # print 'GUESSES', zip(row, guesses_tuples)
        _columns.append(max(guesses_tuples, key=lambda t_n: t_n[1])[0])
    return _columns


def types_processor(types, strict=False):
    """ Apply the column types set on the instance to the
    current row, attempting to cast each cell to the specified
    type.

    Strict means that casting errors are not ignored"""
    def apply_types(row_set, row):
        if types is None:
            return row
        for cell, type in six.moves.zip_longest(row, types):
            try:
                cell.value = type.cast(cell.value)
                cell.type = type
            except:
                if strict and type:
                    raise
        return row
    return apply_types
