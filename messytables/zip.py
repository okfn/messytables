import zipfile

import messytables


class ZIPTableSet(messytables.TableSet):
    """ Reads TableSets from inside a ZIP file """

    def __init__(self, fileobj):
        tables = []
        found = []
        z = zipfile.ZipFile(fileobj, 'r')
        try:
            for f in z.infolist():
                ext = None
                if "." in f.filename:
                    ext = f.filename[f.filename.rindex(".") + 1:]

                try:
                    filetables = messytables.any.any_tableset(z.open(f), extension=ext)
                except ValueError as e:
                    found.append(f.filename + ": " + e.message)
                    continue

                tables.extend(filetables.tables)

            if len(tables) == 0:
                raise ValueError("ZIP file has no recognized tables (%s)." % ", ".join(found))
        finally:
            z.close()
        self._tables = tables

    @property
    def tables(self):
        """ Return the tables contained in any loadable files within the ZIP file. """
        return self._tables
