import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "SSTable",
    version = "0.1.0",
    author = "Anton Kortunov",
    author_email = "toshik@yandex-team.ru",
    description = ("A library that implements sorted string tables "
                                   "with fixed size payload in Python."),
    license = "LGPL",
    keywords = "sstable sorted string table",
    url = "http://github.com/toshic/sstable",
    packages=['sstable', 'tests'],
    long_description=read('README'),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Database",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
    ],
)
