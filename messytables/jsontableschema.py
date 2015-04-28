import json

unicode = type(u'')

class FormatError(Exception): pass
class DuplicateFieldId(Exception): pass

class JSONTableSchema(object):
    """
    A library to handle JSON Table Schema files.
    See http://www.dataprotocols.org/en/latest/json-table-schema.html
    Original source: https://pypi.python.org/pypi/json-table-schema
    This remains licenced under the Apache Licence v2.0
    Original by Martin Keegan - martin.keegan@okfn.org
    Changed by David McKee - dave.mckee@gmail.com
    Modified to make Python 3 compatible
    """

    __valid_type_names__ = [
        "string",
        "number",
        "integer",
        "date",
        "time",
        "date-time",
        "boolean",
        "binary",
        "geopoint",
        "geojson",
        "array",
        "object",
        "any"
        ]

    __format_version__ = 0.1


    required_field_descriptor_keys = ["id", "label", "type"]

    def __init__(self, json_string=None):
        """Initialise JSONTableSchema object, optionally from a JSON string

        param: str json_string - the string from which to initialise

        """
        self.fields = []
        self.format_version = self.__format_version__
        if json_string is not None:
            self.of_string(json_string)
        
    def of_string(self, s):
        self.of_json(json.loads(s))
        
    def of_json(self, j):
        if "fields" not in j:
            raise FormatError("Field `fields' must be present in dictionary")
        field_list = j["fields"]
        if not isinstance(field_list, list):
            raise FormatError("Type of value of key `fields' must be array")

        for idx, stanza in enumerate(field_list):
            if not isinstance(stanza, dict):
                err_str = "Field descriptor %d must be a dictionary" % idx
                raise FormatError(err_str)

            for k in required_field_descriptor_keys:
                if k not in stanza:
                    err_tmpl = "Field descriptor %d must contain key `%s'"
                    raise FormatError(err_tmpl % (idx, k))

            self.add_field(field_id=stanza["id"], 
                           label=stanza["label"], 
                           field_type=stanza["type"])

        self.format_version = j.get("json_table_schema_version",
                                    self.__format_version__)

    @property
    def field_ids(self):
        return [ i["id"] for i in self.fields ]
            
    def add_field(self, field_id=None, label=None, field_type=None):
        if not isinstance(field_id, (str, unicode)):
            raise FormatError("Field `id' must be a string")
        if not isinstance(label, (str, unicode)):
            raise FormatError("Field `label' must be a string")
        if not isinstance(field_type, (str, unicode)):
            raise FormatError("Field `type' must be a string")

        if field_id in self.field_ids:
            raise DuplicateFieldId("field_id")
            
        if field_type not in self.__valid_type_names__:
            err_tmpl = "Invalid type `%s' in field descriptor for `%s'"
            raise FormatError(err_tmpl % (field_type, label))

        self.fields.append({
                "id": field_id,
                "label": label,
                "type": field_type
                })
        
    def remove_field(self, field_id):
        if field_id not in self.field_ids:
            raise KeyError
        self.fields = filter(lambda i: i["id"] != field_id, self.fields)

    def as_json(self):
        return json.dumps(self.as_dict(), indent=2)

    def as_dict(self):
        return {
            "json_table_schema_version": self.format_version,
            "fields": self.fields
            }
