import csv
import re
import codecs
import datetime
import decimal
import itertools
from StringIO import StringIO

## from python documentation
class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f, 'ignore')
    def __iter__(self):
        return self

    def next(self):
        line = self.reader.readline()
        if not line or line == '\0':
            raise StopIteration
        result = line.encode("utf-8")
        return result

class UnicodeReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        self.line_num = self.reader.line_num
        if not row:
            raise StopIteration
        return [s.decode("utf-8") for s in row]

    def __iter__(self):
        return self


def create_date_formats(day_first=True):
    """generate combinations of time and date formats with different delimeters"""

    if day_first:
        date_formats = "dd/mm/yyyy yyyy/mm/dd".split()
        python_date_formats = "%d/%m/%Y %Y/%m/%d".split()
    else:
        date_formats = "mm/dd/yyyy yyyy/mm/dd".split()
        python_date_formats = "%m/%d/%Y %Y/%m/%d".split()
    both_date_formats = zip(date_formats, python_date_formats)

    #time_formats = "hh:mmz hh:mm:ssz hh:mmtzd hh:mm:sstzd".split()
    time_formats = "hh:mm:ssz hh:mm:sstzd".split()
    python_time_formats = "%H:%M%Z %H:%M:%S%Z %H:%M%z %H:%M:%S%z".split()
    both_time_fromats = zip(time_formats, python_time_formats)

    #date_seperators = ["-","."," ","","/","\\"]
    date_seperators = ["-",".","/"]

    all_date_formats = []

    for seperator in date_seperators:
        for date_format, python_date_format in both_date_formats:
            all_date_formats.append(
                (
                 date_format.replace("/", seperator),
                 python_date_format.replace("/", seperator)
                )
            )

    all_formats = {}

    for date_format, python_date_format in all_date_formats:
        all_formats[date_format] = python_date_format
        for time_format, python_time_format in both_time_fromats:

            all_formats[date_format + time_format] = \
                    python_date_format + python_time_format

            all_formats[date_format + "T" + time_format] =\
                    python_date_format + "T" + python_time_format

            all_formats[date_format + " " + time_format] =\
                    python_date_format + " " + python_time_format
    return all_formats

DATE_FORMATS = create_date_formats()

POSSIBLE_TYPES = ["int", "bool", "decimal"] + DATE_FORMATS.keys()

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

    def skip_line_rows(self):

        if not self.guessed_skip_lines or not self.skip_lines:
            return []

        if self.buffer:
            flat_file = StringIO(self.buffer)
        else:
            flat_file = open(self.path, mode = "rb")
        reader = codecs.getreader(self.encoding)(flat_file, 'ignore')

        results = []
        
        for num, line in enumerate(reader):
            result = {}
            result.update(dict((h, None) for h in self.headings))
            result["__errors"] = dict(error="skipped_line",
                                      original_line=line)
            results.append(result)

        return results


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

class TypeGuesser(object):

    def __init__(self, iterable, guess_lines = 1000):
        
        self.iterable = iterable
        self.guess_lines = guess_lines


    def guess(self):

        possible_types = set(POSSIBLE_TYPES)

        max_length = 0

        for num, value in enumerate(self.iterable):
            #if len(line) != len(self.headings):
            #    continue
            max_length = max(max_length, len(value))
            if not value:
                continue
            for type in list(possible_types):
                if type == "int":
                    if not self.is_int(value):
                        possible_types.remove("int")
                elif type == "bool":
                    if not self.is_bool(value):
                        possible_types.remove("bool")
                elif type == "decimal":
                    if not self.is_decimal(value):
                        possible_types.remove("decimal")
                else:
                    python_format = DATE_FORMATS[type]
                    if not self.is_date_format(value, python_format):
                        possible_types.remove(type)


            if num > self.guess_lines:
                check = self.check_possible_types(possible_types)
                if possible_types == set():
                    break
                elif check:
                    return check

        if not possible_types:
            return min(max_length * 7, 2000)
        return self.check_possible_types(possible_types)

    def is_int(self, val):

        try:
            val = int(val)
            if val > 1000000000000:
                return False
            return True
        except ValueError:
            return False

    def is_decimal(self, val):
        try:
            val =  decimal.Decimal(val)
            if val > 1000000000000:
                return False
            return True
        except decimal.InvalidOperation:
            decimal.InvalidOperation
            return False

    def is_bool(self, val):
        if val.lower() in "1 true 0 false".split():
            return True
        return False

    def is_date_format(self, val, date_format):
        try:
            date = datetime.datetime.strptime(val, date_format)
            if date.year > 3000:
                return False
            return True
        except ValueError:
            return False

    def check_possible_types(self, possible_types):

        if (len(possible_types) == 3 and
            "int" in possible_types and
            "decimal" in possible_types):
            possible_types.remove("int")
            possible_types.remove("decimal")
        if (len(possible_types) == 2 and
            "decimal" in possible_types):
            possible_types.remove("decimal")
        if 'bool' in possible_types:
            return 'bool'
        if len(possible_types) == 2:
            if not (set(possible_types) - set(DATE_FORMATS)):
                return possible_types.pop()
        if len(possible_types) == 1:
            return possible_types.pop()

## {{{ http://code.activestate.com/recipes/576669/ (r18)
## Raymond Hettingers proporsal to go in 2.7
from collections import MutableMapping

class OrderedDict(dict, MutableMapping):

    # Methods with direct access to underlying attributes

    def __init__(self, *args, **kwds):
        if len(args) > 1:
            raise TypeError('expected at 1 argument, got %d', len(args))
        if not hasattr(self, '_keys'):
            self._keys = []
        self.update(*args, **kwds)

    def clear(self):
        del self._keys[:]
        dict.clear(self)

    def __setitem__(self, key, value):
        if key not in self:
            self._keys.append(key)
        dict.__setitem__(self, key, value)

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        self._keys.remove(key)

    def __iter__(self):
        return iter(self._keys)

    def __reversed__(self):
        return reversed(self._keys)

    def popitem(self):
        if not self:
            raise KeyError
        key = self._keys.pop()
        value = dict.pop(self, key)
        return key, value

    def __reduce__(self):
        items = [[k, self[k]] for k in self]
        inst_dict = vars(self).copy()
        inst_dict.pop('_keys', None)
        return (self.__class__, (items,), inst_dict)

    # Methods with indirect access via the above methods

    setdefault = MutableMapping.setdefault
    update = MutableMapping.update
    pop = MutableMapping.pop
    keys = MutableMapping.keys
    values = MutableMapping.values
    items = MutableMapping.items

    def __repr__(self):
        pairs = ', '.join(map('%r: %r'.__mod__, self.items()))
        return '%s({%s})' % (self.__class__.__name__, pairs)

    def copy(self):
        return self.__class__(self)

    @classmethod
    def fromkeys(cls, iterable, value=None):
        d = cls()
        for key in iterable:
            d[key] = value
        return d
## end of http://code.activestate.com/recipes/576669/ }}}



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





