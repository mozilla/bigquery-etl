# Scripts for GLAM ETL

This directory contains an assortment of scripts for managing the state of GLAM
queries. Also refer to the `bqetl glam` CLI tool.

The GLAM glean queries are situated within the `glam_etl` dataset under the
`glam-fenix-dev` project. Each query is prefixed with the namespace of the the
glean application.

## Setup

Make sure you have installed all the dependencies for the project.

```bash
# optional: in the project root, a new virtual environment
python3 -m venv venv
source venv/bin/activate

# install dependencies
pip install --no-deps -r requirements.txt
# install bqetl command-line tool
pip install -e .
```

Ensure that you're logged into GCP services.

```bash
gcloud auth login
gcloud auth application-default
```

The former allows command-line tools from the google-cloud-sdk to run, while the
latter allows the sdk (e.g. the `google-cloud-bigquery` python package) to
run locally.

## Running the main integration test

The `test_glean_org_mozilla_fenix_glam_nightly` script is the main testing
script for this set of GLAM ETL queries. The volume of data is typically low and
requires joining across several different glean datasets. Read the script to
determine how to run it. For reference, here is how to generate the the queries
and the schemas for check-in.

```bash
GENERATE_ONLY=true script/glam/test/test_glean_org_mozilla_fenix_glam_nightly
```

The script `test_glean_all_fenix` is the same script, but using the full set of
all Firefox for Android document types. `diff_glean_all_fenix_incremental`
generates the set of SQL and generates a diff between two revisions of the
repository.

After generating and updating the schemas for the tables, it is helpful to
commit the changes into source control to make it easier to distinguish changes
between revisions of the codebase. Only commit the subset of queries that are
representative, since there would be too much duplication otherwise.

## Adding a new glean application

Adding a new Glean application currently requires a few depedencies:

1. In the configuration (currently located in `bigquery_etl.glam.generate`), add
   a new entry for the application id
2. Define a `build_date_udf` that accepts a build id and returns a datetime.
   This is required as part of visualizing data for GLAM. See
   `mozfun.glam.build_hour_to_datetime` for an example.
3. Test the SQL using `run_glam_sql`, either directly or by writing a script for
   automating parts in the `test/` directory.
4. Add the new application to the `dags/glam_fenix` DAG in `telemetry-airflow`.

### Logical app ids

See [this
document](https://docs.google.com/document/d/1O_hDT8po5iEQcle62fN_eno4liVhZOhFXnKRBAoBaWk/edit#heading=h.vevycvlrjpzz)
for a specification on logical application ids. This was introduced in
[PR#1221](https://github.com/mozilla/bigquery-etl/pull/1221). A logical app id
is only necessary if a single application is split across multiple app names,
e.g. Fenix Nightly being spread across `org-mozilla-fenix`,
`org-mozilla-fenix-aurora`, and `org-mozilla-fenix-nightly` depending on the
date. The views for the logical app can be found under
`bigquery_etl/glam/templates/logical_app_id`.

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
