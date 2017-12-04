from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
        name = 'sql2sql',
        packages = ['sql2sql'], # this must be the same as the name above
        version = '1.0.1',
        description = 'A simple lightweight tool to perform ETL jobs between two SQL databases',
        author = 'Joey Sham',
        author_email = 'sham.joey@gmail.com',
        url = 'https://github.com/joeyism/sql2sql', # use the URL to the github repo
        download_url = 'https://github.com/joeyism/sql2sql/dist/1.0.1.tar.gz',
        keywords = ['sql','ETL', 'transform', 'load', 'extract', 'database', 'mysql', 'oracle', 'postgresql', 'psql', 'relational', 'lightweight'],
        classifiers = [],
        install_requires=[],
        )
