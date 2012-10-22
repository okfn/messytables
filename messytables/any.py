import magic, StringIO

from messytables import TableSet, ZIPTableSet
from messytables import CSVTableSet, XLSTableSet, XLSXTableSet

class AnyTableSet(TableSet):
    """ Reads any supported table type according to a specified
    MIME type or file extension or automatically detecting the
    type by using the magic library which looks at the first few
    bytes of the file."""

    @staticmethod
    def make_stream_seekable(fileobj):
        try:
            fileobj.seek(0)
            # if we got here, the stream is seekable
        except:
            # otherwise seek failed, so slurp in stream and wrap
            # it in a StringIO
            fileobj = StringIO.StringIO(fileobj.read())
        return fileobj

    @classmethod
    def from_fileobj(cls, fileobj, mimetype=None, extension=None):
        """ Opens whatever sort of file is passed in, using the MIME
        type (e.g mimetype='text/csv') or file extension (e.g.
        extension='tsv'), or otherwise autodetecting the file format.
        Consult the source for recognized MIME types and file
        extensions."""
        if mimetype == None:
            # Since we need to peek the start of the stream, make sure we can
            # seek back later. If not, slurp in the contents into a StringIO.
            fileobj = make_stream_seekable(fileobj)
            header = fileobj.read(1024)
            mimetype = magic.from_buffer(header, mime=True)
            fileobj.seek(0)

        if mimetype in ('application/x-zip-compressed', 'application/zip') \
                or (extension and extension.lower() in ('zip',)):
            # Do this first because the extension applies to the content
            # type of the inner files, so don't check them before we check
            # for a ZIP file.
            return ZIPTableSet.from_fileobj(fileobj)

        if mimetype in ('text/csv', 'text/comma-separated-values') or \
                (extension and extension.lower() in ('csv',)):
            return CSVTableSet.from_fileobj(fileobj, delimiter=',')
        if mimetype in ('text/tsv', 'text/tab-separated-values') or \
                (extension and extension.lower() in ('tsv',)):
            return CSVTableSet.from_fileobj(fileobj, delimiter='\t')
        if mimetype in ('application/ms-excel', 'application/vnd.ms-excel',
                'application/xls') or (extension and extension.lower() in \
                    ('xls',)):
            return XLSTableSet.from_fileobj(fileobj)
        if mimetype in ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',) \
                or (extension and extension.lower() in ('xlsx',)):
            return XLSXTableSet.from_fileobj(fileobj)

        if mimetype:
            raise ValueError("Unrecognized MIME type: " + mimetype)
        if extension:
            raise ValueError("Could not determine MIME type and "
             + "unrecognized extension: " + extension)
        raise ValueError("Could not determine MIME type and no extension given.")

