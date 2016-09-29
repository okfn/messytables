from messytables import (ZIPTableSet, PDFTableSet, CSVTableSet, XLSTableSet,
                         HTMLTableSet, ODSTableSet)
import messytables
import re


MIMELOOKUP = {'application/x-zip-compressed': 'ZIP',
              'application/zip': 'ZIP',
              'text/comma-separated-values': 'CSV',
              'application/csv': 'CSV',
              'text/csv': 'CSV',
              'text/tab-separated-values': 'TAB',
              'application/tsv': 'TAB',
              'text/tsv': 'TAB',
              'application/ms-excel': 'XLS',
              'application/xls': 'XLS',
              'application/vnd.ms-excel': 'XLS',
              'application/octet-stream': 'XLS', # libmagic detects sw_gen as this on mac
                                                 # with text "Microsoft OOXML"
              'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'XLS',
              'application/vnd.openxmlformats-officedocument.spreadsheetml.sheetapplication/zip': 'XLS',
              'text/html': 'HTML',
              'application/xml': 'HTML', # XHTML is often served as application-xml
              'application/pdf': 'PDF',
              'text/plain': 'CSV',  # could be TAB.
              'application/CDFV2-corrupt': 'XLS',
              'application/CDFV2-unknown': 'XLS',
              'application/vnd.oasis.opendocument.spreadsheet': 'ODS',
              'application/x-vnd.oasis.opendocument.spreadsheet': 'ODS',
              }

def TABTableSet(fileobj):
    return CSVTableSet(fileobj, delimiter='\t')

parsers = {'TAB': TABTableSet,
           'ZIP': ZIPTableSet,
           'XLS': XLSTableSet,
           'HTML': HTMLTableSet,
           'CSV': CSVTableSet,
           'ODS': ODSTableSet,
           'PDF': PDFTableSet}


def clean_ext(filename):
    """Takes a filename (or URL, or extension) and returns a better guess at
    the extension in question.
    >>> clean_ext("")
    ''
    >>> clean_ext("tsv")
    'tsv'
    >>> clean_ext("FILE.ZIP")
    'zip'
    >>> clean_ext("http://myserver.info/file.xlsx?download=True")
    'xlsx'
    """
    dot_ext = '.' + filename
    matches = re.findall('\.(\w*)', dot_ext)
    return matches[-1].lower()


def get_mime(fileobj):
    import magic
    # Since we need to peek the start of the stream, make sure we can
    # seek back later. If not, slurp in the contents into a StringIO.
    fileobj = messytables.seekable_stream(fileobj)
    header = fileobj.read(4096)
    mimetype = magic.from_buffer(header, mime=True)
    fileobj.seek(0)
    if MIMELOOKUP.get(mimetype) == 'ZIP':
        # consider whether it's an Microsoft Office document
        if b"[Content_Types].xml" in header:
            return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    # There's an issue with vnd.ms-excel being returned from XLSX files, too.
    if mimetype == 'application/vnd.ms-excel' and header[:2] == b'PK':
        return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    return mimetype


def guess_mime(mimetype):
    # now returns a clean 'extension' as a string, not a function to call.
    found = MIMELOOKUP.get(mimetype)
    if found:
        return found

    # But some aren't mimetyped due to being buggy but load fine!
    fuzzy_lookup = {'Composite Document File V2 Document': 'XLS'}
    for candidate in fuzzy_lookup:
        if candidate in mimetype:
            return fuzzy_lookup[candidate]


def guess_ext(ext):
    # returns a clean extension as a string, not a function to call.
    lookup = {'zip': 'ZIP',
              'csv': 'CSV',
              'tsv': 'TAB',
              'xls': 'XLS',
              'xlsx': 'XLS',
              'htm': 'HTML',
              'html': 'HTML',
              'pdf': 'PDF',
              'xlt': 'XLS',
                # obscure Excel extensions taken from
                # http://en.wikipedia.org/wiki/List_of_Microsoft_Office_filename_extensions
              'xlm': 'XLS',
              'xlsm': 'XLS',
              'xltx': 'XLS',
              'xltm': 'XLS',
              'ods': 'ODS'}
    if ext in lookup:
        return lookup.get(ext, None)


def any_tableset(fileobj, mimetype=None, extension='', auto_detect=True, **kw):
    """Reads any supported table type according to a specified
    MIME type or file extension or automatically detecting the
    type.

    Best matching TableSet loaded with the fileobject is returned.
    Matching is done by looking at the type (e.g mimetype='text/csv'), then
    the file extension (e.g. extension='tsv'), then autodetecting the
    file format by using the magic library which looks at the first few
    bytes of the file BUT is often wrong. Consult the source for recognized
    MIME types and file extensions.

    On error it raises messytables.ReadError
    """

    short_ext = clean_ext(extension)
    # Auto-detect if the caller has offered no clue. (Because the
    # auto-detection routine is pretty poor.)
    error = []

    if mimetype is not None:
        attempt = guess_mime(mimetype)
        if attempt:
            return parsers[attempt](fileobj, **kw)
        else:
            error.append(
                'Did not recognise MIME type given: "{mimetype}".'.format(
                    mimetype=mimetype))

    if short_ext is not '':
        attempt = guess_ext(short_ext)
        if attempt:
            return parsers[attempt](fileobj, **kw)
        else:
            error.append(
                'Did not recognise extension "{ext}" (given "{full})".'.format(
                    ext=short_ext, full=extension))

    if auto_detect:
        magic_mime = get_mime(fileobj)
        attempt = guess_mime(magic_mime)
        if attempt:
            return parsers[attempt](fileobj, **kw)
        else:
            error.append(
                'Did not recognise detected MIME type: "{mimetype}".'.format(
                    mimetype=magic_mime))

    if error:
        raise messytables.ReadError('any: \n'.join(error))
    else:
        raise messytables.ReadError("any: Did not attempt any detection.")


class AnyTableSet:
    '''Deprecated - use any_tableset instead.'''
    @staticmethod
    def from_fileobj(fileobj, mimetype=None, extension=None):
        return any_tableset(fileobj, mimetype=mimetype, extension=extension)
