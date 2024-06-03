#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="eam-integrations",  # TODO: change placeholder name
    version="0.1.0",
    author="Julio Moscon",
    packages=find_packages(include=["scripts"]),  # TODO: change placeholder name
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
