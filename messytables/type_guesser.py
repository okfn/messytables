from datetime import datetime
import decimal

from messytables.dateparser import DATE_FORMATS

POSSIBLE_TYPES = ["int", "bool", "decimal"] + DATE_FORMATS.keys()

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
            return True
        except ValueError:
            return False

    def is_decimal(self, val):
        try:
            val =  decimal.Decimal(val)
            return True
        except decimal.InvalidOperation:
            decimal.InvalidOperation
            return False

    def is_bool(self, val):
        if val.lower() in "1 true yes 0 false no".split():
            return True
        return False

    def is_date_format(self, val, date_format):
        try:
            datetime.strptime(val, date_format)
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

