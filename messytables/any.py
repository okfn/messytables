from messytables import ZIPTableSet
from messytables import CSVTableSet, XLSTableSet, XLSXTableSet
import messytables


def any_tableset(fileobj, mimetype=None, extension=None):
    """Reads any supported table type according to a specified
    MIME type or file extension or automatically detecting the
    type.

    Best matching TableSet loaded with the fileobject is returned.
    Matching is done by looking at the type (e.g mimetype='text/csv')
    or file extension (e.g. extension='tsv'), or otherwise autodetecting
    the file format by using the magic library which looks at the first few
    bytes of the file. Consult the source for recognized MIME types and file
    extensions."""
    if mimetype is None:
        import magic
        # Since we need to peek the start of the stream, make sure we can
        # seek back later. If not, slurp in the contents into a StringIO.
        fileobj = messytables.seekable_stream(fileobj)
        header = fileobj.read(1024)
        mimetype = magic.from_buffer(header, mime=True)
        fileobj.seek(0)

    if mimetype in ('application/x-zip-compressed', 'application/zip') \
            or (extension and extension.lower() in ('zip',)):
        # Do this first because the extension applies to the content
        # type of the inner files, so don't check them before we check
        # for a ZIP file.
        return ZIPTableSet(fileobj)

    if mimetype in ('text/csv', 'text/comma-separated-values') or \
            (extension and extension.lower() in ('csv',)):
        return CSVTableSet(fileobj)  # guess delimiter
    if mimetype in ('text/tsv', 'text/tab-separated-values') or \
            (extension and extension.lower() in ('tsv',)):
        return CSVTableSet(fileobj, delimiter='\t')
    if mimetype in ('application/ms-excel', 'application/vnd.ms-excel',
            'application/xls') or (extension and extension.lower() in \
                ('xls',)):
        return XLSTableSet(fileobj)
    if mimetype in ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',) \
            or (extension and extension.lower() in ('xlsx',)):
        return XLSXTableSet(fileobj)

    if mimetype:
        raise ValueError("Unrecognized MIME type: " + mimetype)
    if extension:
        raise ValueError("Could not determine MIME type and "
         + "unrecognized extension: " + extension)
    raise ValueError("Could not determine MIME type and no extension given.")

class AnyTableSet:
    '''Deprecated - use any_tableset instead.'''
    @staticmethod
    def from_fileobj(fileobj, mimetype=None, extension=None):
        return any_tableset(fileobj, mimetype=mimetype, extension=extension)
