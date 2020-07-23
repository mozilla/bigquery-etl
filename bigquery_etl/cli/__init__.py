import click

from bigquery_etl.cli.query import query_cli


def cli():
    """Create the bigquery-etl CLI."""
    commands = {
        "query": query_cli,
    }

    @click.group(commands=commands)
    # todo: version
    def group():
        "CLI tools for working with bigquery-etl."

    group()


if __name__ == "__main__":
    cli()
