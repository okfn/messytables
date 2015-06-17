import sys
PY2 = sys.version_info[0] == 2
if PY2:
    import urllib2
    from itertools import izip_longest
    unicode_string = unicode
    native_string = str
    byte_string = str
    string_types = (str, unicode)
    urlopen = urllib2.urlopen
else:  # i.e. PY3
    import urllib.request
    from itertools import zip_longest as izip_longest
    unicode_string = str
    native_string = str
    byte_string = bytes
    
    string_types = (str,)
    urlopen = urllib.request.urlopen
