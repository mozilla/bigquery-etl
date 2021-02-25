# bqetl CLI

The `bqetl` command-line tool aims to simplify working with the bigquery-etl repository by supporting common workflows, such as creating, validating and scheduling queries or adding new UDFs.

## Installation

Please follow the [Quick Start](https://github.com/mozilla/bigquery-etl#quick-start) for setting up bigquery-etl and the bqetl CLI.

## Commands

To list all available commands in the bqetl CLI:

```bash
$ ./bqetl

Usage: bqetl [OPTIONS] COMMAND [ARGS]...

  CLI tools for working with bigquery-etl.

Options:
  --version  Show the version and exit.
  --help     Show this message and exit.

Commands:
  alchemer    Commands for importing alchemer data.
  dag         Commands for managing DAGs.
  dependency  Build and use query dependency graphs.
  dryrun      Dry run SQL.
  format      Format SQL.
  glam        Tools for GLAM ETL.
  mozfun      Commands for managing mozfun routines.
  query       Commands for managing queries.
  routine     Commands for managing routines.
  stripe      Commands for Stripe ETL.
  view        Commands for managing views.
```


See help for any command:

```bash
$ ./bqetl [command] --help
```

### `alchemer`

Commands for importing alchemer data in BigQuery.

#### `backfill`

```bash
# Import data from alchemer (surveygizmo) surveys into BigQuery.
# The date range is inclusive of the start and end values.
$ ./bqetl alchemer backfill --start-date=2021-01-01 --end-date=2021-02-01 --survey_id=xxxxxxxxxxx --api_token=xxxxxxxxxxxxxx --api_secret=xxxxxxxxxxxxxxx --destination_table=moz-fx-data-shared-prod.telemetry_derived.survey_gizmo_daily_attitudes
```

### `dag`

Commands for managing Airflow DAGs.

#### `create`

When creating new DAGs, the DAG name must have a `bqetl_` prefix. Created DAGs are added to the `dags.yaml` file.

```bash
# Create new DAG
$ ./bqetl dag create bqetl_core --schedule-interval="0 2 * * *" --owner=example@mozilla.com --description="Tables derived from the legacy telemetry `core` ping sent by various mobile applications." --start-date=2019-07-25


# Create DAG and overwrite default settings
$ ./bqetl dag create bqetl_ssl_ratios --schedule-interval="0 2 * * *" --owner=example@mozilla.com --description="The DAG schedules SSL ratios queries." --start-date=2019-07-20 --email=example2@mozilla.com,example3@mozilla.com --retries=2 --retry_delay=30m
```

#### `generate`

When generating DAGs, the Airflow DAGs are created based on DAG and task definitions in bigquery-etl and written to the `dags/` directory. This command requires Java to be installed.

```bash
# Generate all DAGs
$ ./bqetl dag generate

# Generate a specific DAG
$ ./bqetl dag generate bqetl_ssl_ratios
```

#### `info`

Get information about available DAGs.

```bash
# Get information about all available DAGs
$ ./bqetl dag info

# Get information about a specific DAG
$ ./bqetl dag info bqetl_ssl_ratios

# Get information about a specific DAG including scheduled tasks
$ ./bqetl dag info --with_tasks bqetl_ssl_ratios
```

#### `remove`

Remove a DAG. This will also remove the scheduling information from the queries that were scheduled as part of the DAG.

```bash
# Remove a specific DAG
$ ./bqetl dag remove bqetl_vrbrowser
```

### dependency

### dryrun

### format

### glam

### mozfun

### query

### routine

### stripe

### view
