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
1. PR gets reviewed and eventually approved
1. Merge pull-request
1. Table deploys happen on a nightly cadence through the [`bqetl_artifact_deployment` Airflow DAG](https://workflow.telemetry.mozilla.org/dags/bqetl_artifact_deployment/grid)
   * Clear the most recent DAG run once a new version of bigquery-etl has been deployed to create new datasets earlier
1. Backfill data
   * Option 1: via Airflow interface
   * Option 2: `./bqetl query backfill --project-id <project id> <dataset>.<table>_<version>`

## Update an existing query

1. Open the `query.sql` file of the query to be updated and make changes
1. Run `./bqetl query validate <dataset>.<table>_<version>` to dry run and format the query
1. If the query scheduling metadata has changed, run `./bqetl dag generate <bqetl_dag>` to update the DAG file
1. If the query adds new columns, run `./bqetl query schema update <dataset>.<table>_<version>` to make local `schema.yaml` updates
1. Open PR with changes
1. PR reviewed and approved
1. Merge pull-request
1. Table deploys (including schema changes) happen on a nightly cadence through the [`bqetl_artifact_deployment` Airflow DAG](https://workflow.telemetry.mozilla.org/dags/bqetl_artifact_deployment/grid)
   * Clear the most recent DAG run once a new version of bigquery-etl has been deployed to apply changes earlier

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
   * For data scientists (and anyone without `jobs.create` permissions in `moz-fx-data-shared-prod`), run:
     * (a) `gcloud auth login --update-adc   # to authenticate to GCP`
     * (b) `gcloud config set project mozdata    # to set the project`
     * (c) `./bqetl query validate --use-cloud-function=false --project-id=mozdata <full path to the query file>`
1. Run `./bqetl query schema update <dataset>.<table> --update_downstream` to make local schema.yaml updates and update schemas of downstream dependencies.
   * [x] This requires [GCP access](https://docs.telemetry.mozilla.org/cookbooks/bigquery/access.html#bigquery-access-request).
   * [x] `--update_downstream` is optional as it takes longer. It is recommended when you know that there are downstream dependencies whose `schema.yaml` need to be updated, in which case, the update will happen automatically.
   * [x] `--force` should only be used in very specific cases, particularly the `clients_last_seen` tables. It skips some checks that would otherwise catch some error scenarios.
1. Open a new PR with these changes.
1. PR reviewed and approved.
1. Find and run again the [CI pipeline](https://app.circleci.com/pipelines/github/mozilla/bigquery-etl?) for the PR.
   * [x] Make sure all dry runs are successful.
1. Merge pull-request.
1. Table deploys happen on a nightly cadence through the [`bqetl_artifact_deployment` Airflow DAG](https://workflow.telemetry.mozilla.org/dags/bqetl_artifact_deployment/grid)
   * Clear the most recent DAG run once a new version of bigquery-etl has been deployed to apply changes earlier

The following is an example to update a new field in `telemetry_derived.clients_daily_v6`

### Example: Add a new field to clients_daily

1. Open the `clients_daily_v6` `query.sql` file and add new field definitions.
1. Run `./bqetl format sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/query.sql`
1. Run `./bqetl query validate telemetry_derived.clients_daily_v6`.
1. Authenticate to GCP: `gcloud auth login --update-adc`
1. Run `./bqetl query schema update telemetry_derived.clients_daily_v6 --update_downstream --ignore-dryrun-skip --use-cloud-function=false`.
   * [x] `schema.yaml` files of downstream dependencies, like `clients_last_seen_v1` are updated.
   * If the schema has no changes, we do not run schema updates on any of its downstream dependencies.
   * `--use-cloud-function=false` is necessary when updating tables related to `clients_daily` but optional for other tables. The dry run cloud function times out when fetching the deployed table schema for some of `clients_daily`s downstream dependencies. Using GCP credentials instead works, however this means users need to have permissions to run queries in `moz-fx-data-shared-prod`.
1. Open a PR with these changes.
1. PR is reviewed and approved.
1. Merge pull-request.
1. Table deploys happen on a nightly cadence through the [`bqetl_artifact_deployment` Airflow DAG](https://workflow.telemetry.mozilla.org/dags/bqetl_artifact_deployment/grid)
   * Clear the most recent DAG run once a new version of bigquery-etl has been deployed to apply changes earlier

## Remove a field from a table schema

Deleting a field from an existing table schema should be done only when is totally neccessary. If you decide to delete it:
1. Validate if there is data in the column and make sure data it is either backed up or it can be reprocessed.
1. Follow [Big Query docs](https://cloud.google.com/bigquery/docs/managing-table-schemas#deleting_columns_from_a_tables_schema_definition) recommendations for deleting.
1. If the column size exceeds the allowed limit, consider setting the field as NULL. See this [search_clients_daily_v8](https://github.com/mozilla/bigquery-etl/pull/2463) PR for an example.

## Adding a new mozfun UDF

1. Run `./bqetl mozfun create <dataset>.<name> --udf`.
2. Navigate to the `udf.sql` file in `sql/mozfun/<dataset>/<name>/` and add UDF the definition and tests.
3. Run `./bqetl mozfun validate <dataset>.<name>` for formatting and running tests.
   * Before running the tests, you need to [setup the access to the Google Cloud API](https://mozilla.github.io/bigquery-etl/cookbooks/testing/).
4. Open a PR.
5. PR gets reviewed, approved and merged.
6. To publish UDF immediately:
   * Go to Airflow `mozfun` DAG and clear latest run.
   * Or else it will get published within a day when mozfun is executed next.

## Adding a new internal UDF

Internal UDFs are usually only used by specific queries. If your UDF might be useful to others consider publishing it as a `mozfun` UDF.

1. Run `./bqetl routine create <dataset>.<name> --udf`
2. Navigate to the `udf.sql` in `sql/moz-fx-data-shared-prod/<dataset>/<name>/` file and add UDF definition and tests
3. Run `./bqetl routine validate <dataset>.<name>` for formatting and running tests
   * Before running the tests, you need to [setup the access to the Google Cloud API](https://mozilla.github.io/bigquery-etl/cookbooks/testing/).
5. Open a PR
6. PR gets reviewed and approved and merged
1. UDF deploys happen on a nightly cadence through the [`bqetl_artifact_deployment` Airflow DAG](https://workflow.telemetry.mozilla.org/dags/bqetl_artifact_deployment/grid)
   * Clear the most recent DAG run once a new version of bigquery-etl has been deployed to apply changes earlier

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

## Using a private internal UDF

1. Follow the steps for Adding a new internal UDF above to create a stub of the private UDF. Note this should *not*
contain actual private UDF code or logic. The directory name and function parameters should match the private UDF.
1. **Do Not** publish the stub UDF. This could result in incorrect results for other users of the private UDF.
1. Open a PR
1. PR gets reviewed, approved and merged

## Creating a new BigQuery Dataset

To provision a new BigQuery dataset for holding tables, you'll need to
create a `dataset_metadata.yaml` which will cause the dataset to be
automatically deployed [after merging](https://docs.telemetry.mozilla.org/concepts/pipeline/artifact_deployment.html#dataset-deployment).
Changes to existing
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

1. Run `./bqetl docs generate --output_dir generated_docs`
1. Navigate to the `generated_docs` directory
1. Run `mkdocs serve` to start a local `mkdocs` server.


## Setting up change control to code files

Each code files in the bigquery-etl repository can have a set of owners who are responsible to review and approve changes, and are automatically assigned as PR reviewers.
The query files in the repo also benefit from the metadata labels to be able to validate and identify the data that is change controlled.

Here is a [sample PR with the implementation of change control for contextual services data](https://github.com/mozilla/bigquery-etl/pull/3833).

1. Select or create a [Github team or identity](https://docs.github.com/en/organizations/organizing-members-into-teams/creating-a-team) and add the GitHub emails of the query codeowners. A GitHub identity is particularly useful when you need to include non @mozilla emails or to randomly assign PR reviewers from the team members. This team requires edit permissions to bigquery-etl, to achieve this, inherit the team from one that has the required permissions e.g. `mozilla > telemetry`.
1. Open the `metadata.yaml` for the query where you want to apply change control:
   * In the section `owners`, add the selected GitHub identity, along with the list of owners' emails.
   * In the section `labels`, add `change_controlled: true`. This enables identifying change controlled data in the BigQuery console and in the Data Catalog.
1. Setup the `CODEOWNERS`:
   * Open the `CODEOWNERS` file located in the root of the repo.
   * Add a new row with the path and owners for the query. You can place it in the corresponding section or create a new section in the file, e.g. `/sql_generators/active_users/templates/ @mozilla/kpi_table_reviewers`.
1. The queries labeled change_controlled are automatically validated in the CI. To run the validation locally:
   * Run the command `script/bqetl query validate <query_path>`.
   * If the query is generated using the `/sql-generators`, first run `./script/bqetl generate <path>` and then run `script/bqetl query validate <query_path>`.
