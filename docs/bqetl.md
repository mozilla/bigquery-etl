# bqetl CLI

The `bqetl` command-line tool aims to simplify working with the bigquery-etl repository by supporting common workflows, such as creating, validating and scheduling queries or adding new UDFs.

Running some commands, for example to create or query tables, will [require Mozilla GCP access](https://docs.telemetry.mozilla.org/cookbooks/bigquery/access.html#bigquery-access-request).

## Installation

Follow the [Quick Start](https://github.com/mozilla/bigquery-etl#quick-start) to set up bigquery-etl and the bqetl CLI.

## Configuration

`bqetl` can be configured via the `bqetl_project.yaml` file. See [Configuration](https://mozilla.github.io/bigquery-etl/reference/configuration/) to find available configuration options.

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
  backfill    Commands for managing backfills.
```


See help for any command:

```bash
$ ./bqetl [command] --help
```

<!--- Documentation for comments is generated via ./bqetl docs generate -->
