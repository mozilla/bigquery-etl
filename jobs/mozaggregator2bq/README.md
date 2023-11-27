# mozaggregator2bq

A set of scripts for loading Firefox Telemetry aggregates into BigQuery. These
aggregates power the Telemetry Dashboard and Evolution Viewer.

This job was imported from
[mozilla/mozaggregator2bq](https://github.com/mozilla/mozaggregator2bq). See the
archived repository for historical commit logs.

## Overview

Build the container and launch it:

```bash
docker-compose build
docker-compose run --rm app bash
```

### Running a notebook

Ensure your data directory in the top-level directory matches the one in the
notebook. Run the following script.

```bash
bin/start-jupyter
```

This script can be modified to include various configuration parameters for
spark, including the default parallelism and the amount of executor memory.

### Processing `pg_dump` into parquet

Run the following scripts to transform the data dumps into parquet, where the
json fields have been transformed into appropriate columns and arrays.

```bash
bin/submit-local bin/pg_dump_to_parquet.py \
    --input-dir data/submission_date/20191201 \
    --output-dir data/parquet/submission_date/20191201
```

### Running backfill

The `bin/backfill` script will dump data from the Postgres database, transform
the data into Parquet, and load the data into a BigQuery table. The current
schema for the table is as follows:

Field name | Type | Mode
-|-|-
ingest_date | DATE | REQUIRED
aggregate_type | STRING | NULLABLE
ds_nodash | STRING | NULLABLE
channel | STRING | NULLABLE
version | STRING | NULLABLE
os | STRING | NULLABLE
child | STRING | NULLABLE
label | STRING | NULLABLE
metric | STRING | NULLABLE
osVersion | STRING | NULLABLE
application | STRING | NULLABLE
architecture | STRING | NULLABLE
aggregate | STRING | NULLABLE

There is a table for the build id aggregates and the submission date aggregates.
The build ids are truncated to the nearest date.

It may be useful to use a small entry-point script.

```bash
#!/bin/bash
set -x
start=2015-06-01
end=2020-04-01
while ! [[ $start > $end ]]; do
    rm -r data
    up_to=$(date -d "$start + 1 month" +%F)
    START_DS=$start END_DS=$up_to bash -x bin/backfill
    start=$up_to
done
```
