'''
Convert a rowset to the json table schema (http://www.dataprotocols.org/en/latest/json-table-schema.html)
'''

import messytables
import jsontableschema

MESSYTABLES_TO_JTS_MAPPING = {
    messytables.StringType: 'string',
    messytables.IntegerType: 'integer',
    messytables.FloatType: 'number',
    messytables.DecimalType: 'number',
    messytables.DateType: 'date',
    messytables.DateUtilType: 'date'
}


def celltype_as_string(celltype):
    return MESSYTABLES_TO_JTS_MAPPING[celltype.__class__]


def rowset_as_jts(rowset, headers=None, types=None):
    ''' Create a json table schema from a rowset
    '''
    _, headers = messytables.headers_guess(rowset.sample)
    types = map(celltype_as_string, messytables.type_guess(rowset.sample))

    return headers_and_typed_as_jts(headers, types)


def headers_and_typed_as_jts(headers, types):
    ''' Create a json table schema from headers and types as
    returned from :meth:`~messytables.headers.headers_guess` and :meth:`~messytables.types.type_guess`.
    '''
    j = jsontableschema.JSONTableSchema()

    for field_id, field_type in zip(headers, types):
        j.add_field(field_id=field_id,
                    label=field_id,
                    field_type=field_type)

    return j
