from setuptools import setup, find_packages

long_desc = """
Tabular data as published on the web is often not well formatted
and structured. Messytables tries to detect and fix errors in the
data. Typical examples include:

* Finding the header of a table when there are explanations and
  text fragments in the first few rows of the table.
* Guessing the type of columns in CSV data.

This library provides data structures and some heuristics to
fix these problems and read a wide number of different tabular
abominations.

See the full documentation at: http://messytables.readthedocs.org
"""


setup(
    name='messytables',
    version='0.11.0',
    description="Parse messy tabular data in various formats",
    long_description=long_desc,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
    ],
    keywords='',
    author='Open Knowledge Foundation',
    author_email='info@okfn.org',
    url='http://okfn.org',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=[],
    include_package_data=False,
    zip_safe=False,
    install_requires=[
        'xlrd>=0.8.0',
        'python-magic==0.4.3',  # used for type guessing
        'openpyxl==1.5.7',
        'chardet==2.1.1',
        'python-dateutil>=1.5.0,<2.0.0',
        'json-table-schema',
        'lxml'
    ],
    tests_require=[],
    entry_points=\
    """
    """,
)
