
def offset_processor(offset):
    """ Skip ``offset`` from the given iterator. This can
    be used in combination with the ``headers_processor`` to
    apply the result of a header scan to the table.

    :param offset: Offset to be skipped
    :type offset: int
    """
    def apply_offset(row_set, row):
        if not hasattr(row_set, '_offset'):
            row_set._offset = 0
        if row_set._offset >= offset:
            return row
        row_set._offset += 1
    return apply_offset


def null_processor(nulls):
    """ Replaces every occurrence of items from `nulls` with None.

    :param nulls: List of items to be replaced
    :type nulls: list
    """
    def apply_replace(row_set, row):
        def replace(cell):
            if cell.value in nulls:
                cell.value = None
            return cell
        return [replace(cell) for cell in row]
    return apply_replace
