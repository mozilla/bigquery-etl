#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="dap-collector-job",
    version="0.1.0",
    author="sfriedberger@mozilla.com",
    packages=find_packages(include=["dap_collector"]),
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
