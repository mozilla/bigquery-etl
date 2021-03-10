# Common bigquery-etl workflows

This is a quick guide of how to perform common workflows in bigquery-etl using the `bqetl` CLI.

For any workflow, the bigquery-etl repositiory needs to be locally available, for example by cloning the repository, and the `bqetl` CLI needs to be installed by running `./bqetl bootstrap`.

## Adding a new scheduled query

The [Creating derived datasets tutorial](https://mozilla.github.io/bigquery-etl/cookbooks/creating_a_derived_dataset/) provides a more detailed guide on creating scheduled queries.

1. Run `./bqetl query create <dataset>.<table>_<version>`
    * Specify the desired destination dataset and table name for `<dataset>.<table>_<version>`
    * Directories and files are generated automatically 
1. Open `query.sql` file that has been created in `sql/moz-fx-data-shared-prod/<dataset>/<table>_<version>/` to write the query
1. [Optional] Run `./bqetl query schema update <dataset>.<table>_<version>` to generate the `schema.yaml` file
    * Optionally add column descriptions to `schema.yaml`
1. Open the `metadata.yaml` file in `sql/moz-fx-data-shared-prod/<dataset>/<table>_<version>/`
    * Add a description of the query
    * Add BigQuery information such as table partitioning or clustering
        * See [clients_daily_v6](https://github.com/mozilla/bigquery-etl/blob/master/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/metadata.yaml) for reference
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
    * Option 2: `./bqetl query backfill <dataset>.<table>_<version>`

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

## Publishing data

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
