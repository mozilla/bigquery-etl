#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="quicksuggest2bq",
    version="0.1.0",
    author="Mozilla Corporation",
    packages=find_packages(include=["quicksuggest2bq"]),
    python_requires=">=3.8.0",
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
