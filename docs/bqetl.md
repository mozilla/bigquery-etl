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

<!--- Documentation for comments is generated via bigquery_etl/docs/generate_docs -->
