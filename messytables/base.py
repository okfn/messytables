"""
Changes from dataproxy module::

    * removed all references to auditing and calls to the brewery.dq module
"""
transformers = []

def register_transformer(transformer):
    transformers.append(transformer)
    
def find_transformer(extension = None, mime_type = None):
    if not extension and not mime_type:
        raise ValueError("Either extension or mime type should be specified")

    info = None
    for trans in transformers:
        if extension and extension in trans["extensions"]:
            info = trans
        elif extension and extension in trans["mime_types"]:
            info = trans
        if mime_type and mime_type in trans["mime_types"]:
            info = trans
    if not info:
        return None

    return info["class"]

def transformer(type_name):
    """Get transformation module for resource of given type"""
    trans_class = find_transformer(extension = type_name)
    if not trans_class:
        raise Exception("No transformer for type '%s'" % type_name)
    return trans_class()

class Transformer(object):
    """Data resource transformer - abstract ckass"""
    def __init__(self):
        self.requires_size_limit = True
        self.max_results = None

    def read_source_rows(self, src):
        rows = []
        record_count = 0
    
        for row in src.rows():
            rows.append(row)
            record_count += 1
            if self.max_results and record_count >= self.max_results:
                break

        result = {
            "fields": src.field_names,
            "data": rows
        }
        if self.max_results:
            result["max_results"] = self.max_results
        return result
