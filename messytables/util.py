
def offset_processor(offset):
    """ Skip ``offset`` from the given iterator. This can 
    be used in combination with the ``headers_processor`` to 
    apply the result of a header scan to the table. """
    i = [0]
    def apply_offset(row_set, row):
        if i[0] >= offset:
            return row
        i[0] += 1
    return apply_offset
