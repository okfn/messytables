import sys
PY2 = sys.version_info[0] == 2
if PY2:
    import urllib2
    native_string = unicode
    string_types = (str, unicode)
    urlopen = urllib2.urlopen
else:  # i.e. PY3
    import urllib.request
    native_string = str
    string_types = (str,)
    urlopen = urllib.request.urlopen
