0.11.0 (master, wip)
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
