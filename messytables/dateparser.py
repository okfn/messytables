import re

date_regex = re.compile(r'''^\d{1,4}[-\/\.\s]\S+[-\/\.\s]\S+''')


def is_date(value):
    return len(value) != 1 and date_regex.match(value)


def create_date_formats(day_first=True):
    """generate combinations of time and date
    formats with different delimeters
    """

    if day_first:
        date_formats = ['dd/mm/yyyy', 'dd/mm/yy', 'yyyy/mm/dd']
        python_date_formats = ['%d/%m/%Y', '%d/%m/%y', '%Y/%m/%d']
    else:
        date_formats = ['mm/dd/yyyy', 'mm/dd/yy', 'yyyy/mm/dd']
        python_date_formats = ['%m/%d/%Y', '%m/%d/%y', '%Y/%m/%d']

    date_formats += [
        # Things with words in
        'dd/bb/yyyy', 'dd/bbb/yyyy'
    ]
    python_date_formats += [
        # Things with words in
        '%d/%b/%Y', '%d/%B/%Y'
    ]

    both_date_formats = list(zip(date_formats, python_date_formats))

    #time_formats = "hh:mmz hh:mm:ssz hh:mmtzd hh:mm:sstzd".split()
    time_formats = "hh:mm:ssz hh:mm:ss hh:mm:sstzd".split()
    python_time_formats = "%H:%M%Z %H:%M:%S %H:%M:%S%Z %H:%M%z %H:%M:%S%z".split()
    both_time_formats = list(zip(time_formats, python_time_formats))

    #date_separators = ["-","."," ","","/","\\"]
    date_separators = ["-", ".", "/", " "]

    all_date_formats = []

    for separator in date_separators:
        for date_format, python_date_format in both_date_formats:
            all_date_formats.append(
                (date_format.replace("/", separator),
                 python_date_format.replace("/", separator))
            )

    all_formats = {}

    for date_format, python_date_format in all_date_formats:
        all_formats[date_format] = python_date_format
        for time_format, python_time_format in both_time_formats:

            all_formats[date_format + time_format] = \
                python_date_format + python_time_format

            all_formats[date_format + "T" + time_format] =\
                python_date_format + "T" + python_time_format

            all_formats[date_format + " " + time_format] =\
                python_date_format + " " + python_time_format
    return list(all_formats.values())

DATE_FORMATS = create_date_formats()
