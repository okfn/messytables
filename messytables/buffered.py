import io

BUFFER_SIZE = 4096


def seekable_stream(fileobj):
    try:
        fileobj.seek(0)
        # if we got here, the stream is seekable
        return fileobj
    except:
        # otherwise seek failed, so slurp in stream and wrap
        # it in a BytesIO
        return BufferedFile(fileobj)


class BufferedFile(object):
    """A buffered file that preserves the beginning of a stream."""

    def __init__(self, fp, buffer_size=BUFFER_SIZE + 2):
        self.data = io.BytesIO()
        self.fp = fp
        self.offset = 0
        self.len = 0
        self.fp_offset = 0
        self.buffer_size = buffer_size

    def _next_line(self):
        try:
            return self.fp.readline()
        except AttributeError:
            return next(self.fp)

    def _read(self, n):
        return self.fp.read(n)

    @property
    def _buffer_full(self):
        return self.len >= self.buffer_size

    def readline(self):
        if self.len < self.offset < self.fp_offset:
            raise BufferError('Line is not available anymore')
        if self.offset >= self.len:
            line = self._next_line()
            self.fp_offset += len(line)

            self.offset += len(line)

            if not self._buffer_full:
                self.data.write(line)
                self.len += len(line)
        else:
            line = self.data.readline()
            self.offset += len(line)
        return line

    def read(self, n=-1):
        if n == -1:
            # if the request is to do a complete read, then do a complete
            # read.
            self.data.seek(self.offset)
            return self.data.read(-1) + self.fp.read(-1)

        if self.len < self.offset < self.fp_offset:
            raise BufferError('Data is not available anymore')
        if self.offset >= self.len:
            byte = self._read(n)
            self.fp_offset += len(byte)

            self.offset += len(byte)

            if not self._buffer_full:
                self.data.write(byte)
                self.len += len(byte)
        else:
            byte = self.data.read(n)
            self.offset += len(byte)
        return byte

    def tell(self):
        return self.offset

    def seek(self, offset):
        if self.len < offset < self.fp_offset:
            raise BufferError('Cannot seek because data is not buffered here')
        self.offset = offset
        if offset < self.len:
            self.data.seek(offset)
