
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
    return all_formats.values()

DATE_FORMATS = create_date_formats()

