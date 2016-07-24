import codecs
try:
    import cchardet as chardet
except ImportError:
    import chardet

from messytables.buffered import BUFFER_SIZE

# maps between chardet encoding and codecs bom keys
BOM_MAPPING = {
    'utf-16le': 'BOM_UTF16_LE',
    'utf-16be': 'BOM_UTF16_BE',
    'utf-32le': 'BOM_UTF32_LE',
    'utf-32be': 'BOM_UTF32_BE',
    'utf-8': 'BOM_UTF8',
    'utf-8-sig': 'BOM_UTF8',
}


def analyze_stream(stream, encoding=None):
    sample = stream.read(BUFFER_SIZE)
    if encoding is None:
        encoding = chardet.detect(sample).get('encoding') or 'utf-8'
    encoding = encoding.lower()
    # The reader only skips a BOM if the encoding isn't explicit about its
    # endianness (i.e. if encoding is UTF-16 a BOM is handled properly
    # and taken out, but if encoding is UTF-16LE a BOM is ignored).
    # However, if chardet sees a BOM it returns an encoding with the
    # endianness explicit, which results in the codecs stream leaving the
    # BOM in the stream. This is ridiculously dumb. For UTF-{16,32}{LE,BE}
    # encodings, check for a BOM and remove it if it's there.
    if encoding in BOM_MAPPING:
        bom = getattr(codecs, BOM_MAPPING[encoding], None)
        if sample[:len(bom)] == bom:
            return encoding, sample[len(bom):]
    return encoding, sample
