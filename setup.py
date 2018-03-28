import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "caterpie",
    version = "0.0.4",
    author = "Saad Khan",
    author_email = "skhan8@mail.einstein.yu.edu",
    description = ("ORM for uploading CSVs to a postgresql database with python and pandas."),
    license = "GPL",
    keywords = "orm database",
    #url = "http://packages.python.org/an_example_pypi_project",
    packages=['caterpie', 'test'],
    #long_description=read('README'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: GPL License",
    ],
)
