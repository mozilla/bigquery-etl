# Common bigquery-etl workflows

This is a quick guide of how to perform common workflows in bigquery-etl using the `bqetl` CLI.

For any workflow, the bigquery-etl repositiory needs to be locally available, for example by cloning the repository, and the `bqetl` CLI needs to be installed by running `./bqetl bootstrap`.

## Adding a new scheduled query

The [Creating derived datasets tutorial](https://mozilla.github.io/bigquery-etl/cookbooks/creating_a_derived_dataset/) provides a more detailed guide on creating scheduled queries.

1. Run `./bqetl query create <dataset>.<table>_<version>`
   1. Specify the desired destination dataset and table name for `<dataset>.<table>_<version>`
   1. Directories and files are generated automatically
1. Open `query.sql` file that has been created in `sql/moz-fx-data-shared-prod/<dataset>/<table>_<version>/` to write the query
1. [Optional] Run `./bqetl query schema update <dataset>.<table>_<version>` to generate the `schema.yaml` file
    * Optionally add column descriptions to `schema.yaml`
1. Open the `metadata.yaml` file in `sql/moz-fx-data-shared-prod/<dataset>/<table>_<version>/`
    * Add a description of the query
    * Add BigQuery information such as table partitioning or clustering
        * See [clients_daily_v6](https://github.com/mozilla/bigquery-etl/blob/main/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/metadata.yaml) for reference
1. Run `./bqetl query validate <dataset>.<table>_<version>` to dry run and format the query
1. To schedule the query, first select a DAG from the `./bqetl dag info` list or create a new DAG `./bqetl dag create <bqetl_new_dag>`
1. Run `./bqetl query schedule <dataset>.<table>_<version> --dag <bqetl_dag>` to schedule the query
1. Run `./bqetl dag generate <bqetl_dag>` to update the DAG file
1. Create a pull request
    * CI fails since table doesn't exist yet
1. PR gets reviewed and eventually approved
1. Create destination table: `./bqetl query schema deploy` (requires a `schema.yaml` file to be present)
     * This step needs to be performed by a data engineer as it requires DE credentials.
1. Merge pull-request
1. Backfill data
     * Option 1: via Airflow interface
     * Option 2: `./bqetl query backfill --project-id <project id> <dataset>.<table>_<version>`

## Update an existing query

1. Open the `query.sql` file of the query to be updated and make changes
1. Run `./bqetl query validate <dataset>.<table>_<version>` to dry run and format the query
1. If the query scheduling metadata has changed, run `./bqetl dag generate <bqetl_dag>` to update the DAG file
1. If the query adds new columns, run `./bqetl query schema update <dataset>.<table>_<version>` to make local `schema.yaml` updates
1. Open PR with changes
    * CI can fail if schema updates haven't been propagated to destination tables, for example when adding new fields
1. PR reviewed and approved
1. Deploy schema changes by running `./bqetl query schema deploy <dataset>.<table>_<version>`
1. Merge pull-request

## Formatting SQL

We enforce consistent SQL formatting as part of CI. After adding or changing a
query, use `./bqetl format` to apply formatting rules.

Directories and files passed as arguments to `./bqetl format` will be
formatted in place, with directories recursively searched for files with a
`.sql` extension, e.g.:

```bash
$ echo 'SELECT 1,2,3' > test.sql
$ ./bqetl format test.sql
modified test.sql
1 file(s) modified
$ cat test.sql
SELECT
  1,
  2,
  3
```

If no arguments are specified the script will read from stdin and write to
stdout, e.g.:

```bash
$ echo 'SELECT 1,2,3' | ./bqetl format
SELECT
  1,
  2,
  3
```

To turn off sql formatting for a block of SQL, wrap it in `format:off` and
`format:on` comments, like this:

```sql
SELECT
  -- format:off
  submission_date, sample_id, client_id
  -- format:on
```

## Add a new field to a table schema
Adding a new field to a table schema also means that the field has to propagate to several downstream tables, which makes it a more complex case.

1. Open the `query.sql` file inside the `<dataset>.<table>` location and add the new definitions for the field.
1. Run `./bqetl format <path to the query>` to format the query. Alternatively, run `./bqetl format $(git ls-tree -d HEAD --name-only)` validate the format of all queries that have been modified.
1. Run `./bqetl query validate <dataset>.<table>` to dry run the query.
1. Run `./bqetl query schema update <dataset>.<table> --update_downstream` to make local schema.yaml updates and update schemas of downstream dependencies.
   * [x] This requires [GCP access](https://docs.telemetry.mozilla.org/cookbooks/bigquery/access.html#bigquery-access-request).
   * [x] Note that schema.yaml files of downstream dependencies will be automatically updated.
1. Open a new PR with these changes.
1. The dry-run-sql task is expected to fail at this point due to mismatch with deployed schemas!
1. PR reviewed and approved.
1. Deploy schema changes by running: `./bqetl query schema deploy <dataset>.<table>;`
1. Rerun the CI pipeline in the PR.
   * [x] Make sure all dry runs are successful.
1. Merge pull-request.

The following is an example to update a new field in telemetry_derived.clients_daily_v6

### Example: Add a new field to clients_daily

1. Open the `clients_daily_v6` `query.sql` file and add new field definitions.
1. Run `./bqetl format sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/query.sql`
1. Run `./bqetl query validate telemetry_derived.clients_daily_v6`.
1. Run `./bqetl query schema update telemetry_derived.clients_daily_v6 --update_downstream`.
    * [x] `schema.yaml` files of downstream dependencies, like `clients_last_seen_v1` are updated.
1. Open a PR with these changes.
    * [x] The `dry-run-sql` task fails.
1. PR is reviewed and approved.
1. Deploy schema changes by running:
   ```
   ./bqetl query schema deploy telemetry_derived.clients_daily_v6;
   ./bqetl query schema deploy telemetry_derived.clients_daily_joined_v1;
   ./bqetl query schema deploy --force telemetry_derived.clients_last_seen_v1;
   ./bqetl query schema deploy telemetry_derived.clients_last_seen_joined_v1;
   ./bqetl query schema deploy --force telemetry_derived.clients_first_seen_v1;
   ```
1. Rerun CI pipeline
   * [x] All tests pass
1. Merge pull-request.

## Remove a field from a table schema

Deleting a field from an existing table schema should be done only when is totally neccessary. If you decide to delete it:
* [x] Validate if there is data in the column and make sure data it is either backed up or it can be reprocessed.
* Follow [Big Query docs](https://cloud.google.com/bigquery/docs/managing-table-schemas#deleting_columns_from_a_tables_schema_definition) recommendations for deleting.
* If the column size exceeds the allowed limit, consider setting the field as NULL. See this [search_clients_daily_v8](https://github.com/mozilla/bigquery-etl/pull/2463) PR for an example.

## Adding a new mozfun UDF

1. Run `./bqetl mozfun create <dataset>.<name> --udf`
1. Navigate to the `udf.sql` file in `sql/mozfun/<dataset>/<name>/` and add UDF the definition and tests
1. Run `./bqetl mozfun validate <dataset>.<name>` for formatting and running tests
1. Open a PR
1. PR gets reviewed and approved and merged
1. To publish UDF immediately:
    * Go to Airflow `mozfun` DAG and clear latest run
    * Or else it will get published within a day when mozfun is executed next

## Adding a new internal UDF

Internal UDFs are usually only used by specific queries. If your UDF might be useful to others consider publishing it as a `mozfun` UDF.

1. Run `./bqetl routine create <dataset>.<name> --udf`
1. Navigate to the `udf.sql` in `sql/moz-fx-data-shared-prod/<dataset>/<name>/` file and add UDF definition and tests
1. Run `./bqetl routine validate <dataset>.<name>` for formatting and running tests
1. Open a PR
1. PR gets reviewed and approved and merged
1. To publish UDF immediately:
    * Run `./bqetl routine publish`
    * Or else it will take a day until UDF gets published automatically

## Adding a stored procedure

The same steps as creating a new UDF apply for creating stored procedures, except when initially creating the procedure execute `./bqetl mozfun create <dataset>.<name> --stored_procedure` or `./bqetl routine create <dataset>.<name> --stored_procedure` for internal stored procedures.

## Updating an existing UDF

1. Navigate to the `udf.sql` file and make updates
1. Run `./bqetl mozfun validate <dataset>.<name>` or `./bqetl routine validate <dataset>.<name>` for formatting and running tests
1. Open a PR
1. PR gets reviewed, approved and merged

## Renaming an existing UDF

1. Run `./bqetl mozfun rename <dataset>.<name> <new_dataset>.<new_name>`
    * References in queries to the UDF are automatically updated
1. Open a PR
1. PR gets reviews, approved and merged

## Creating a new BigQuery Dataset

To provision a new BigQuery dataset for holding tables, you'll need to
create a `dataset_metadata.yaml` which will cause the dataset to be
automatically deployed a few hours after merging. Changes to existing
datasets may trigger manual operator approval (such as changing access policies).
For more on access controls, see
[Data Access Workgroups](https://mana.mozilla.org/wiki/display/DOPS/Data+Access+Workgroups)
in Mana.

The `bqetl query create` command will automatically generate a skeleton
`dataset_metadata.yaml` file if the query name contains a dataset that
is not yet defined.

See example with commentary for `telemetry_derived`:

```yaml
friendly_name: Telemetry Derived
description: |-
  Derived data based on pings from legacy Firefox telemetry, plus many other
  general-purpose derived tables
labels: {}

# Base ACL should can be:
#   "derived" for `_derived` datasets that contain concrete tables
#   "view" for user-facing datasets containing virtual views
dataset_base_acl: derived

# Datasets with user-facing set to true will be created both in shared-prod
# and in mozdata; this should be false for all `_derived` datasets
user_facing: false

# Most datasets can have mozilla-confidential access like below, but some
# datasets will be defined with more restricted access or with additional
# access for services; see "Data Access Workgroups" link above.
workgroup_access:
- role: roles/bigquery.dataViewer
  members:
  - workgroup:mozilla-confidential
```

## Publishing data

See also the reference for [Public Data](../reference/public_data.md).

1. Get a data review by following the [data publishing process](https://wiki.mozilla.org/Data_Publishing#Dataset_Publishing_Process_2)
1. Update the `metadata.yaml` file of the query to be published
    * Set `public_bigquery: true` and optionally `public_json: true`
    * Specify the `review_bugs`
1. If an internal dataset already exists, move it to `mozilla-public-data`
1. If an `init.sql` file exists for the query, change the destination project for the created table to `mozilla-public-data`
1. Run `./bqetl dag generate bqetl_public_public_data_json` to update the DAG
1. Open a PR
1. PR gets reviewed, approved and merged
    * Once, ETL is running a view will get automatically published to `moz-fx-data-shared-prod` referencing the public dataset

## Adding new Python requirements

When adding a new library to the Python requirements, first add the library to
the requirements and then add any meta-dependencies into constraints.
Constraints are discovered by installing requirements into a fresh virtual
environment. A dependency should be added to either `requirements.txt` or
`constraints.txt`, but not both.

```bash
# Create a python virtual environment (not necessary if you have already
# run `./bqetl bootstrap`)
python3 -m venv venv/

# Activate the virtual environment
source venv/bin/activate

# If not installed:
pip install pip-tools --constraint requirements.in

# Add the dependency to requirements.in e.g. Jinja2.
echo Jinja2==2.11.1 >> requirements.in

# Compile hashes for new dependencies.
pip-compile --generate-hashes requirements.in

# Deactivate the python virtual environment.
deactivate
```

## Making a pull request from a fork

When opening a pull-request to merge a fork, the `manual-trigger-required-for-fork` CI task will
fail and some integration test tasks will be skipped. A user with repository write permissions
will have to run the [Push to upstream workflow](https://github.com/mozilla/bigquery-etl/actions/workflows/push-to-upstream.yml)
and provide the `<username>:<branch>` of the fork as parameter. The parameter will also show up
in the logs of the `manual-trigger-required-for-fork` CI task together with more detailed instructions.
Once the workflow has been executed, the CI tasks, including the integration tests, of the PR will be
executed.

## Building the Documentation

The [repository documentation](https://mozilla.github.io/bigquery-etl/) is built using [MkDocs](https://www.mkdocs.org/).
To generate and check the docs locally:

1. Run `script/generate_docs --output_dir generated_docs`
1. Navigate to the `generated_docs` directory
1. Run `mkdocs serve` to start a local `mkdocs` server.
