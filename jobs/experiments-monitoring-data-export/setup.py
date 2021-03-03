#!/usr/bin/env python

# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="experiments-monitoring-data-export",
    description="Scripts for exporting experiments monitoring data to GCS.",
    version="0.1.0",
    author="Anna Scholtz",
    author_email="ascholtz@mozilla.com",
    packages=find_packages(include=["experiments_monitoring_data_export"]),
    package_dir={"experiments-monitoring-data-export": "experiments_monitoring_data_export"},
    long_description=readme,
    include_package_data=True,
    license="Mozilla",
)
