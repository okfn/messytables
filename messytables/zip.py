import zipfile

from messytables.core import TableSet
from messytables.error import ReadError


class ZIPTableSet(TableSet):
    """Reads TableSets from inside a ZIP file."""

    def __init__(self, fileobj, **kw):
        """On error it will raise ReadError."""
        from messytables.any import any_tableset
        tables = []
        found = []
        z = zipfile.ZipFile(fileobj, 'r')
        try:
            for f in z.infolist():
                ext = None

                # ignore metadata folders added by Mac OS X
                if '__MACOSX' in f.filename:
                    continue

                if "." in f.filename:
                    ext = f.filename[f.filename.rindex(".") + 1:]

                try:
                    filetables = any_tableset(z.open(f), extension=ext, **kw)
                except ValueError as e:
                    found.append(f.filename + ": " + e.message)
                    continue

                tables.extend(filetables.tables)

            if len(tables) == 0:
                raise ReadError('''ZIP file has no recognized tables (%s).'''
                                % ', '.join(found))
        finally:
            z.close()

        self._tables = tables
