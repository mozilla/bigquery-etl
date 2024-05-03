#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="dap-collector-ppa-dev-job",
    version="0.1.0",
    author="bbirdsong@mozilla.com",
    packages=find_packages(include=["dap_collector_ppa_dev"]),
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
