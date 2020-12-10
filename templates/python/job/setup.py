#!/usr/bin/env python

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="docker-etl-job",  # TODO: change placeholder name
    version="0.1.0",
    author="Mozilla Corporation",
    packages=find_packages(include=["docker_etl"]), # TODO: change placeholder name
    long_description=readme,
    include_package_data=True,
    license="MPL 2.0",
)
