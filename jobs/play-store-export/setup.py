#!/usr/bin/env python

# -*- coding: utf-8 -*-

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from setuptools import setup, find_packages

readme = open("README.md").read()

setup(
    name="play_store_export",
    description="Scripts to export Play Store app data to BigQuery using Transfer Service",
    author="Ben Wu",
    author_email="bewu@mozilla.com",
    url="https://github.com/mozilla/leanplum_data_export",
    packages=find_packages(include=["play_store_export"]),
    package_dir={"play-store-export": "play_store_export"},
    python_requires=">=3.6.0",
    version="0.1.0",
    long_description=readme,
    include_package_data=True,
    license="Mozilla",
)
