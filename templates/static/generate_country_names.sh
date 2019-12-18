#!/bin/bash

# Combine country_codes_v1 with country_names_alternate.csv
(cat country_codes_v1/data.csv && sed 1d country_names_alternate.csv) > country_names_v1/data.csv
