#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="influxdb-to-bigquery",
    version="0.1.0",
    author="akommasani@mozilla.com",
    packages=find_packages(include=["influxdb_to_bigquery"]),
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
