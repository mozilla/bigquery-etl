#!/bin/bash

# Creates static tables for mapping between country codes and country names
# by loading data from CSV files.

cd "$(dirname "$0")"

# List of ISO 3166 alpha-2 country codes and names generated via:
# curl -sL https://datahub.io/core/country-list/r/data.csv > country_codes.csv
# See https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2


## Load country_codes_v1

COUNTRY_CODES_SCHEMA='
[
  {
    "name": "name",
    "description": "Official country name per ISO 3166",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "code",
    "description": "ISO 3166 alpha-2 country code",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
'

bq load \
   --ignore_unknown_values \
   --skip_leading_rows=1 \
   --source_format=CSV \
   --schema=<(echo "$COUNTRY_CODES_SCHEMA") \
   --replace \
   moz-fx-data-derived-datasets:static.country_codes_v1 \
   country_codes.csv

bq update \
   --description "Mapping of ISO 3166 alpha-2 country codes to country names; country codes are unique" \
   moz-fx-data-derived-datasets:static.country_codes_v1

## Load country_names_v1

COUNTRY_NAMES_SCHEMA='
[
  {
    "name": "name",
    "description": "An official or common country name; a single country may appear with multiple names",
    "type": "STRING",
    "mode": "REQUIRED"
  },
  {
    "name": "code",
    "description": "ISO 3166 alpha-2 country code",
    "type": "STRING",
    "mode": "REQUIRED"
  }
]
'

(cat country_codes.csv && sed 1d country_names_alternate.csv) > country_names_full.csv

bq load \
   --ignore_unknown_values \
   --skip_leading_rows=1 \
   --source_format=CSV \
   --schema=<(echo "$COUNTRY_NAMES_SCHEMA") \
   --replace \
   moz-fx-data-derived-datasets:static.country_names_v1 \
   country_names_full.csv

bq update \
   --description="Mapping of country names to ISO 3166 alpha-2 country codes; the same code may appear under multiple names" \
   moz-fx-data-derived-datasets:static.country_names_v1
