#!/bin/bash

# This is a script intended to be run from the query directory;
# it reads in the query.sql and outputs a modified version with params
# that can be understood by BQ scheduled queries

cd "$(dirname "$0")"

sed 's/@submission_date/DATE_SUB(@run_date, INTERVAL 1 DAY)/g' query.sql
