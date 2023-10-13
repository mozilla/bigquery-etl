#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="kpi_forecasting",
    version="1.0.0",
    author="Mozilla Corporation",
    packages=find_packages(),
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
