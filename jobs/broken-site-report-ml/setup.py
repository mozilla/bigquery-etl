#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="broken-site-report-ml",
    version="0.1.0",
    author="Mozilla Corporation",
    packages=find_packages(include=["broken_site_report_ml"]),
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
