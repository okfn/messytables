
Parsing for messy tables
========================

Often, tabular table is prepared in a way that does not lend itself 
to easy machine extraction: random header columns, encoding issues, 
incorrect or missing type information are just some of the common 
pitfalls. ``messytables`` is a library that accepts input in various
formats and applies heuristics to guess a correct way of accessing 
the contained information.





DEV: Table Extraction Stages
----------------------------

What stages are required to convert tabular files from various sources into
something that can be further processed (e.g. uploaded to the webstore)?

See also: https://bitbucket.org/johnglover/ckanext-qa/src/80d7f1a047fc/ckanext/qa/lib/transform/

 #. Detect the file format
 #. Select the sheet (if supported)
 #. Iterate over rows correctly, yielding cells
 #. Detect the header column (if present)
 #. Detect column data types
 #. Pad missing values
 #. Detect the end of data (vs. EOF)

