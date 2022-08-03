# SQL generators

`sql_generators/` contains scripts for generating SQL queries. Generated SQL query code should *not* be checked in to `main`. The scripts for generating SQL queries are executed by CI only and are followed by generating Airflow DAGs.

## Expected structure

The directories in `sql_generators/` represent the generated queries and will contain all of the scripts and templates necessary to generate these queries. Each query-specific directory will contain a `__init__.py` file that contains the query generation logic. Optionally, a `templates/` directory can be added which contains the Jinja templates queries are generated from.

Each `__init__.py` file needs to implement a `generate()` method that is configured as a [click command](https://click.palletsprojects.com/en/8.0.x/). The `bqetl` CLI will automatically add these commands to the `./bqetl query generate` command group.

