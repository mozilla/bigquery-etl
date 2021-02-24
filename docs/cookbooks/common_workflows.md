# Common bigquery-etl workflows

This is a quick guide of how to perform common workflows in bigquery-etl using the `bqetl` CLI.

For any workflow, the bigquery-etl repositiory needs to be locally available, for example by cloning the repository, and the `bqetl` CLI needs to be installed by running `./bqetl bootstrap`.

## Adding a new scheduled query

1. Run `./bqetl query create <dataset>.<table>`
    * Specifiy the desired destination dataset and table name for `<dataset>.<table>`
    * Directories and files are generated automatically 
1. Open `query.sql` file that has been created in `sql/moz-fx-data-shared-prod/<dataset>/<table>/` to write the query
1. Run `./bqetl query schema update <dataset>.<table>` to generate the `schema.yaml` file
    * Optionally add column descriptions to `schema.yaml`
1. Open the `metadata.yaml` file in `sql/moz-fx-data-shared-prod/<dataset>/<table>/`
    * Add a description of the query
    * Add BigQuery information such as table partitioning or clustering
        * See [clients_daily_v6](https://github.com/mozilla/bigquery-etl/blob/master/sql/moz-fx-data-shared-prod/telemetry_derived/clients_daily_v6/metadata.yaml) for reference
1. Run `./bqetl query validate <dataset>.<table>` to dry run and format the query
1. To schedule the query, first select a DAG from the `./bqetl dag info` list or create a new DAG `./bqetl dag create <bqetl_new_dag>` 
1. Run `./bqetl query schedule <dataset>.<table> --dag <bqetl_dag>` to schedule the query
1. Run `./bqetl dag generate <bqetl_dag>` to update the DAG file
1. Create a pull request
    * CI fails since table doesn’t exist yet
1. PR gets reviewed and eventually approved
1. Create destination table: `./bqetl query schema deploy`
1. Merge pull-request
1. Backfill data
    * Option 1: via Airflow interface
    * Option 2: `./bqetl query backfill <dataset>.<table>`
