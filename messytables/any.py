from messytables import ZIPTableSet
from messytables import CSVTableSet, XLSTableSet, XLSXTableSet, HTMLTableSet
from messytables.error import ReadError
import messytables
import re


def TABTableSet(fileobj):
    return CSVTableSet(fileobj, delimiter='\t')

priorities = [ZIPTableSet, XLSTableSet, XLSXTableSet,
              HTMLTableSet, TABTableSet, CSVTableSet]


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
    # There's an issue with vnd.ms-excel being returned fro XLSX files, too.
    if mimetype == 'application/vnd.ms-excel' and header[:2] == 'PK':
        return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    return mimetype


def guess_mime(mimetype):
    lookup = {'application/x-zip-compressed': ZIPTableSet,
              'application/zip': ZIPTableSet,
              'text/comma-separated-values': CSVTableSet,
              'text/tab-separated-values': TABTableSet,
              'application/ms-excel': XLSTableSet,
              'application/vnd.ms-excel': XLSTableSet,
              'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': XLSXTableSet,
              'text/html': HTMLTableSet,
              'application/xml': XLSXTableSet,
              'text/plain': CSVTableSet,  # could be TAB.
              'application/CDFV2-corrupt': XLSTableSet,
              }
    if mimetype in lookup:
        return lookup.get(mimetype, None)

    # But some aren't mimetyped due to being buggy but load fine!
    fuzzy_lookup = {'Composite Document File V2 Document': XLSTableSet}
    for candidate in fuzzy_lookup:
        if candidate in mimetype:
            return fuzzy_lookup[candidate]


def guess_ext(ext):
    lookup = {'zip': ZIPTableSet,
              'csv': CSVTableSet,
              'tsv': TABTableSet,
              'xls': XLSTableSet,
              'xlsx': XLSXTableSet,
              'htm': HTMLTableSet,
              'html': HTMLTableSet,
              'xlt': XLSTableSet,
                # obscure Excel extensions taken from
                # http://en.wikipedia.org/wiki/List_of_Microsoft_Office_filename_extensions
              'xlm': XLSTableSet,
              'xlsm': XLSXTableSet,
              'xltx': XLSXTableSet,
              'xltm': XLSXTableSet}
    if ext in lookup:
        return lookup.get(ext, None)


def any_tableset(fileobj, mimetype=None, extension='', auto_detect=True):
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
            return attempt(fileobj)
        else:
            error.append(
                "Did not recognise MIME type given: {mimetype}.".format(
                    mimetype=mimetype))

    if short_ext is not '':
        attempt = guess_ext(short_ext)
        if attempt:
            return attempt(fileobj)
        else:
            error.append(
                "Did not recognise extension {ext} (given {full}.".format(
                    ext=short_ext, full=extension))

    if auto_detect:
        magic_mime = get_mime(fileobj)
        attempt = guess_mime(magic_mime)
        if attempt:
            return attempt(fileobj)
        else:
            error.append(
                "Did not recognise detected MIME type: {mimetype}.".format(
                    mimetype=magic_mime))

    if error:
        raise ReadError('any: \n'.join(error))
    else:
        raise ReadError("any: Did not attempt any detection.")


class AnyTableSet:
    '''Deprecated - use any_tableset instead.'''
    @staticmethod
    def from_fileobj(fileobj, mimetype=None, extension=None):
        return any_tableset(fileobj, mimetype=mimetype, extension=extension)
