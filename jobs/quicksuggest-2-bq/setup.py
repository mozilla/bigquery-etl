#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="quicksuggest_2_bq",
    version="0.1.0",
    author="Mozilla Corporation",
    packages=find_packages(include=["quicksuggest_2_bq"]),
    package_dir={"quicksuggest-2-bq": "quicksuggest_2_bq"},
    python_requires=">=3.8.0",
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
