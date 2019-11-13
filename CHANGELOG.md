* Remove PDF functionality (PDFTableSet)- pdftables is not maintained

0.15.2 (8 February 2017)
* #165: detect ods types: boolean, currency, time and percentage. support repeated columns
* #160: Correct spelling of separator in source

0.15.1 (29 September 2016)
* #158: Add CDFV2-unknown to MIMELOOKUP
* #157: Fix for Python Magic API change
* #156: HTTPSify URLs in layout.html
* #153: Convert readthedocs links for their .org -> .io migration for
  hosted projects
* #147: Min python-dateutil version changed back to 1.5
* #145: Fix upper limit for json-table-schema version
* #143: Updated requirements.
* #141: Don't parse inputs with length 1 as dates #140
* #136: Pass any\_tableset arguments through to specific parsers.

0.15 (19 June 2015)
* #124 Python 2/3 multilingual.
* #125 Fix ODS stopping at blank lines
* #127 Fix reading of newer ODS files
* #130 UTF8Recoder removes UTF8 BOM

0.14.5 (3 March 2015)
* #123 dates
* #120 Better error handling on properties

0.14.2 (5 Feb 2015)
* #119 Detect blank cells
* #118 Fix typo which installed 'test' package.
* #116 Basic richtext support
* #34  Basic richtext support
* #113 Detect pipe delimited files
* #112 Improved tolerance of malformed HTML tables

0.14.1 (1 September 2014)
* Add support for Boolean Type guessing
* SW #25 Ignore invisible text in HTML cells
* Misc Scraperwiki Changes
* Update pdftables version, it in turn requires specific pdfminer version

0.14.0
* Add null processor
* Update magic library

0.13.0 (15 October 2013)
* #86 Provide more information about the original table (properties)
* #89 Skip PDF tests if PDFTables not installed

0.12.0 (7 August 2013)
* #51 Removed FloatType, use DecimalType
* #81 Make HTML table name human-readable
* #82 Add preliminary PDF support
* #83 Use XLSTableSet also for xlsx files

0.11.0 (8 July 2013)
* #65 Reworked any.py
* #60 Basic HTML table parsing
* #55 Fix `__repr__` and unicode issue
* #55 Add `__getitem__` to TableSet

0.10.0 (16 May 2013)
* #52 Rewrite type guessing
* Properly handle empty values when applying types
* Use normal float casting, if possible instead of locale.atof

0.9.0 (6 May 2013)

0.8.0 (6 May 2013)
* #45/#46 `from_fileobj` is deprecated - please use the constructor directly.
* `AnyTableSet` is deprecated - please use the function `any_tableset`.
* #47 Can now open xls with explicit character encoding

0.7.0 (2 May 2013)
* #40 JSON Table Schema output
* improve overall type guessing if the type is already correct
* #30 Fix for type_guess guesses datetime field on xls files as string
* Larger sample when detecting character encoding
* Avoid Python 2.7 dependency (ZipFile)

0.6.0 (10 Apr 2013)
* better CSV dialect sniffing
* Cope with blank first sheet
* #38 Fix for BufferedFile over a network socket
* Skip initial spaces in cells

0.5.0 (19 Feb 2013) - beta

0.4.0 (8 Jan 2013)
