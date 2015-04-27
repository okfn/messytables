import sys
PY2 = sys.version_info[0] == 2
if not PY2:
    native_string = str
    string_types = (str,)
else:
    native_string = unicode
    string_types = (str, unicode)
