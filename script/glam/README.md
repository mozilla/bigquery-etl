# Scripts for GLAM ETL

This directory contains an assortment of scripts for managing the state of GLAM
queries. Also refer to the `bqetl glam` CLI tool.

The GLAM glean queries are situated within the `glam_etl` dataset under the
`glam-fenix-dev` project. Each query is prefixed with the namespace of the the
glean application.

## Running the main integration test

The `test_glean_org_mozilla_fenix_glam_nightly` script is the main testing
script for this set of GLAM ETL queries. The volume of data is typically low and
requires joining across several different glean datasets. Read the script to
determine how to run it. For reference, here is how to generate the the queries
and the schemas for check-in.

```bash
GENERATE_ONLY=true script/glam/test/test_glean_org_mozilla_fenix_glam_nightly
```

## Dropping tables

Use the `list_tables` script to enumerate all of the tables for the GLAM
dataset. For example, we may want to prune all of the tables that do not belong
to the logical glam app ids for Fenix (e.g. `org_mozilla_fenix_glam_nightly`).
We can use grep to limit the set of tables that we want to delete.

```bash
script/glam/list_tables glam_etl | \
    grep -v clients_daily | grep -v fenix_glam | \
    xargs -I {} echo "bq rm -f {}"
```

We generate the commands to drop incremental tables using the following:

```bash
script/glam/list_tables glam_etl | \
    grep -v clients_daily | grep aggregates_v1 | \
    xargs -I {} echo "bq rm -f {}"
```

To copy tables from the `glam_etl` table to the `glam_etl_dev` table, run the
following:

```bash
script/glam/list_tables glam_etl | \
    grep clients_daily | grep -v org_mozilla_firefox | grep -v view | \
xargs -I {} echo bq cp -f {} {} | sed 's/glam_etl/glam_etl_dev/2'
```
