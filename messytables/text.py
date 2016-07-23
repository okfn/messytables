import codecs
try:
    import cchardet as chardet
except ImportError:
    import chardet
from six import text_type, binary_type

from messytables.buffered import BUFFER_SIZE


class UTF8Recoder:
    """Iterator that reads an encoded stream and re-encodes it to UTF-8."""

    # maps between chardet encoding and codecs bom keys
    BOM_MAPPING = {
        'utf-16le': 'BOM_UTF16_LE',
        'utf-16be': 'BOM_UTF16_BE',
        'utf-32le': 'BOM_UTF32_LE',
        'utf-32be': 'BOM_UTF32_BE',
        'utf-8': 'BOM_UTF8',
        'utf-8-sig': 'BOM_UTF8',

    }

    def __init__(self, f, encoding):
        sample = f.read(BUFFER_SIZE)
        if not encoding:
            encoding = chardet.detect(sample).get('encoding') or 'utf-8'
        f.seek(0)
        self.reader = codecs.getreader(encoding)(f, 'ignore')

        # The reader only skips a BOM if the encoding isn't explicit about its
        # endianness (i.e. if encoding is UTF-16 a BOM is handled properly
        # and taken out, but if encoding is UTF-16LE a BOM is ignored).
        # However, if chardet sees a BOM it returns an encoding with the
        # endianness explicit, which results in the codecs stream leaving the
        # BOM in the stream. This is ridiculously dumb. For UTF-{16,32}{LE,BE}
        # encodings, check for a BOM and remove it if it's there.
        if encoding.lower() in self.BOM_MAPPING:
            bom = getattr(codecs, self.BOM_MAPPING[encoding.lower()], None)
            if bom:
                # Try to read the BOM, which is a byte sequence, from
                # the underlying stream. If all characters match, then
                # go on. Otherwise when a character doesn't match, seek
                # the stream back to the beginning and go on.
                for c in bom:
                    if f.read(1) != c:
                        f.seek(0)
                        break

    def __iter__(self):
        return self

    def __next__(self):
        line = self.reader.readline()
        if not line or line == '\0':
            raise StopIteration
        result = line.encode("utf-8")
        return result

    next = __next__


def to_unicode_or_bust(obj, encoding='utf-8'):
    if isinstance(obj, binary_type):
        obj = text_type(obj, encoding)
    return obj
