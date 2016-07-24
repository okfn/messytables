import six
from typecast import guesser, GUESS_TYPES


def type_guess(rows, types=GUESS_TYPES, strict=False):
    """Guess the best type for a given row set.

    The type guesser aggregates the number of successful conversions of each
    column to each type, weights them by a fixed type priority and select the
    most probable type for each column based on that figure. It returns a list
    of ``CellType``. Empty cells are ignored.

    Strict means that a type will not be guessed if parsing fails for a single
    cell in the column.
    """
    guessers = []
    for i, row in enumerate(rows):
        for _ in range(len(row) - len(guessers)):
            guessers.append(guesser(types=types, strict=strict))
        for j, cell in enumerate(row):
            # add string guess so that we have at least one guess
            guessers[j].add(cell.value)
    return [g.best for g in guessers]


def types_processor(types, strict=False):
    """Apply the column types to the each row.

    Strict means that casting errors are not ignored.
    """
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
