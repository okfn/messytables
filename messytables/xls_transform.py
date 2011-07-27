"""Data Proxy - XLS transformation adapter"""
import urllib2
import base
import brewery.ds as ds

try:
    import json
except ImportError:
    import simplejson as json

class XLSTransformer(base.Transformer):
    def __init__(self, url):
        super(XLSTransformer, self).__init__(url)
        # if 'worksheet' in self.query:
        #     self.sheet_number = int(self.query.getfirst('worksheet'))
        # else:
        self.sheet_number = 0
        
    def transform(self):
        handle = urllib2.urlopen(self.url)
        src = ds.XLSDataSource(handle, sheet = self.sheet_number)
        src.initialize()
        result = self.read_source_rows(src)
        handle.close()
        return result
