from setuptools import setup, find_packages

setup(
    name='messytables',
    version='0.1',
    description="Parse messy tabular data in various formats",
    long_description="""\
    """,
    classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
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
        'xlrd==0.7.1'
    ],
    tests_require=[],
    entry_points=\
    """
    """,
)
