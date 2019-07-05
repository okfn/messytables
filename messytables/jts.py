"""Convert a rowset to the json table schema.

(http://www.dataprotocols.org/en/latest/json-table-schema.html)
"""
import jsontableschema

from messytables.headers import headers_guess
from messytables.types import type_guess


def rowset_as_jts(rowset, headers=None, types=None):
    """Create a json table schema from a rowset."""
    _, headers = headers_guess(rowset.sample)
    types = type_guess(rowset.sample)
    types = [t.jts_name for t in types]
    return headers_and_typed_as_jts(headers, types)


def headers_and_typed_as_jts(headers, types):
    """Create a json table schema from headers and types.

    Those specs are returned from :meth:`~messytables.headers.headers_guess`
    and :meth:`~messytables.types.type_guess`.
    """
    jts = jsontableschema.JSONTableSchema()
    for field_id, field_type in zip(headers, types):
        jts.add_field(field_id=field_id,
                      label=field_id,
                      field_type=field_type)
    return jts
