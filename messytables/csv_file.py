import re
import csv
import datetime
import decimal
import itertools
from StringIO import StringIO

from messytables.dateparser import DATE_FORMATS
from messytables.unicsv import UnicodeReader
from messytables.ordereddict import OrderedDict
from messytables.type_guesser import TypeGuesser

class CsvFile(object):

    def __init__(self, path = None, headings = None,
                 format = None, skip_lines = 0,
                 buffer = None, types = None,
                 dialect = None, encoding = "utf-8"):

        self.path = path
        self.buffer = buffer
        self.defined_headings = headings
        self.types = types or {}
        self.file_headings = None
        self.skip_lines = skip_lines
        self.format = format
        self.headings_type = OrderedDict()
        self.headings = []
        self.dialect = dialect
        self.encoding = encoding
        self.has_header = True
        self.guessed_skip_lines = False

        self.guess_lines = 1000

        if not self.format:
            return

        if "quoting" in self.format:
            quoting = self.format["quoting"].upper()
            self.format["quoting"] = getattr(csv, quoting)
        class CustomDialect(csv.excel):
            pass
        for key, value in self.format.iteritems():
            setattr(CustomDialect, key, value)
        self.dialect = CustomDialect

    def guess_skip_lines(self, max=50, guess_lines=50, percent=0.6):

        if self.buffer:
            flat_file = StringIO(self.buffer)
        else:
            flat_file = open(self.path, mode = "rb")

        best_line = 0
        best_percent = 0

        for i in xrange(50):
            flat_file.seek(0)
            for line in range(i):
                flat_file.readline()
            tell = flat_file.tell()
            flat_file.seek(tell)

            sniffer = csv.Sniffer()
            if self.dialect:
                dialect = self.dialect
            else:
                dialect = sniffer.sniff(flat_file.read(20240))
                if dialect.delimiter not in [' ','\t','|',',',';',':']:
                    dialect = csv.excel
                if dialect.delimiter == ' ':
                    dialect.delimiter = ','

            flat_file.seek(tell)
            csv_reader = UnicodeReader(flat_file, dialect, self.encoding)
            slice = itertools.islice(csv_reader, 0, guess_lines)
            good_lines, bad_lines = 0, 0
            first_line = slice.next()
            first_line_len = len([item for item in first_line if item])
            for line in slice:
                if first_line_len == len(line):
                    good_lines += 1
                else:
                    bad_lines += 1
            if bad_lines == 0 and good_lines > 5:
                self.skip_lines = i 
                self.guessed_skip_lines = True
                return
            ## when at end of file
            if bad_lines + good_lines == 0:
                break
            good_percent = good_lines / (bad_lines + good_lines)
            if good_percent > percent and good_percent > best_percent:
                best_percent = good_percent
                best_line = i
        self.skip_lines = best_line
        self.guessed_skip_lines = True

    def get_dialect(self):

        if self.dialect:
            return

        try:
            if self.buffer:
                flat_file = StringIO(self.buffer)
            else:
                flat_file = open(self.path, mode = "rb")
            try:
                flat_file.seek(0)
                for line in range(self.skip_lines):
                    flat_file.readline()
                tell = flat_file.tell()

                sniffer = csv.Sniffer()
                self.dialect = sniffer.sniff(flat_file.read(20240))
                if self.dialect.delimiter not in [' ','\t','|',',',';',':']:
                    raise csv.Error
                flat_file.seek(tell)
                if not self.skip_lines:
                    self.has_header = sniffer.has_header(flat_file.read(20240))
            except csv.Error:
                self.dialect = csv.excel
                self.has_header = True
            if self.dialect.delimiter == ' ':
                self.dialect.delimiter = ','
            if self.buffer:
                flat_file.seek(0)
        finally:
            flat_file.close()


    def get_headings(self):

        if self.defined_headings:
            return

        try:
            flat_file, csv_reader = self.get_csv_reader()
            first_line = csv_reader.next()
            if self.has_header:
                self.file_headings = first_line
            else:
                self.file_headings = [''] * len(first_line)

            unknown_col_num = 0
            for num, heading in enumerate(self.file_headings):
                self.file_headings[num] = re.sub(r'[^a-zA-Z0-9_ -]', '', heading)

                if not heading:
                    self.file_headings[num] = 'column %03d' % unknown_col_num 
                    unknown_col_num += 1
        finally:
            flat_file.close()

    def parse_headings(self):

        headings = self.defined_headings or self.file_headings

        for heading in headings:
            try:
                name, type = heading.split("{")
                type = type.replace("}","")
            except ValueError:
                name, type = heading, None

            if type:
                self.check_type(type)

            self.headings_type[name] = type
            self.headings.append(name)

        if not self.types:
            return

        for heading, type in self.types:
            if heading not in self.headings_type:
                continue
            self.headings_type[heading] = type


    def check_type(self, type):

        if type.lower() in  ("int", "integer",
                          "bool", "boolean",
                          "decimal", "string",
                          "varchar", "text"):
            return
        if type.lower() in DATE_FORMATS:
            return
        try:
            int(type)
        except ValueError:
            raise ValueError("date type %s not valid" % type)

    def column_generator(self, col, flat_file, csv_reader):

        if self.file_headings:
            csv_reader.next()

        for num, line in enumerate(csv_reader):
            if col >= len(self.headings):
                continue
            if col >= len(line):
                continue
            yield line[col]

    def guess_types(self):
        for num, name in enumerate(self.headings):
            type = self.headings_type[name]
            if type:
                continue

            try:
                flat_file, csv_reader = self.get_csv_reader()
                generator = self.column_generator(num, flat_file, csv_reader)
                guessed_type = TypeGuesser(generator).guess()
                if not guessed_type:
                    raise ValueError("unable to guess type for column %s"
                                     % name)
                self.headings_type[name] = guessed_type
            finally:
                flat_file.close()



    def skip(self, csv_reader):

        if self.skip_lines:
            for num, line in enumerate(csv_reader):
                if num == self.skip_lines - 1:
                    return


    def get_csv_reader(self):
        if self.buffer:
            flat_file = StringIO(self.buffer)
        else:
            flat_file = open(self.path, mode = "rb")
        csv_reader = UnicodeReader(flat_file, self.dialect, self.encoding)
        self.skip(csv_reader)
        return flat_file, csv_reader


    def chunk(self, lines):
        try:
            self.lines = lines
            flat_file, csv_reader = self.get_csv_reader()

            if self.file_headings:
                csv_reader.next()

            self.chunks = {}

            chunk = 0
            counter = 0
            total = 0
            offset = flat_file.tell()


            for num, line in enumerate(csv_reader):
                counter = counter + 1
                total = total + 1
                if counter == lines:
                    new_offset = flat_file.tell()
                    self.chunks[chunk] = (offset, new_offset)
                    offset = new_offset
                    counter = 0
                    chunk = chunk + 1
            new_offset = flat_file.tell()
            self.chunks[chunk] = (offset, new_offset)

            return total

        finally:
            if "flat_file" in locals():
                flat_file.close()

    def convert(self, line):

        new_line = []
        error_line = []

        for num, value in enumerate(line):
            heading = self.headings[num]
            type = self.headings_type[heading]
            new_value = None
            if value == '':
                new_line.append(None)
                continue
            try:
                if type == "int":
                    new_value = int(value)
                elif type == "bool":
                    new_value = bool(value)
                elif type == "decimal":
                    new_value = decimal.Decimal(value)
                elif type in DATE_FORMATS:
                    format = DATE_FORMATS[type]
                    new_value = datetime.datetime.strptime(value, format)
                else:
                    new_value = value
            except TypeError:
                new_line.append(value)
                error_line.append('data_type_error')

            new_line.append(new_value)
            error_line.append('')

        return new_line, error_line

    def iterate_csv(self, chunk = None,
                    as_dict = False, convert = False,
                    no_end = False):

        try:
            flat_file, csv_reader = self.get_csv_reader()

            if self.file_headings:
                csv_reader.next()

            if chunk is not None:
                start, end = self.chunks[chunk]
            else:
                start, end = flat_file.tell(), None
            if no_end:
                end = None

            flat_file.seek(start)

            while 1:
                line = csv_reader.next()
                if convert and len(line) == len(self.headings):
                    line, error_line = self.convert(line)
                if not as_dict:
                    stop = (yield line)
                else:
                    result = OrderedDict()
                    errors = OrderedDict()
                    if len(line) != len(self.headings):
                        result.update(dict((h, None) for h in self.headings))
                        result["__errors"] = dict(error="wrong length line",
                                                  original_line=line)
                        stop = (yield result)
                    else:
                        for col_num, value in enumerate(line):
                            result[self.headings[col_num]] = value
                        for col_num, value in enumerate(error_line):
                            if value:
                                errors[self.headings[col_num]] = value
                        result["__errors"] = errors
                        stop = (yield result)
                if stop:
                    break
                if end and end <= flat_file.tell():
                    break

        finally:
            flat_file.close()



if __name__ == "__main__":

    input = """a;b;c
1.5;afdfsaffsa;01012006
2.5;s;01012000
1;b;21012000
1;b;21012000
1;c;01012000"""


    csvfile = CsvFile("wee.txt", format = {"delimiter" : ";"})
    csvfile.get_dialect()
    csvfile.get_headings()
    csvfile.parse_headings()
    csvfile.guess_types()
    
    csvfile.chunk(1)
    print csvfile.headings_type
    print csvfile.chunks



    for line in csvfile.iterate_csv(0, convert = True, as_dict = True, no_end = False):
        print line

    for line in csvfile.iterate_csv(1, convert = True, as_dict = True, no_end = False):
        print line





