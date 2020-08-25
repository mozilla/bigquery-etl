"""bigquery-etl CLI format command."""

import click


@click.command(help="Format SQL.",)
@click.argument(
    "path", default="sql/", type=click.Path(file_okay=True),
)
def format(path):
    """Apply formatting to SQL files."""
    if os.path.isdir(path) and os.path.exists(path):
        sql_files = [
            f for f in glob.glob(path + "/*.sql", recursive=True) if f not in SKIP
        ]
    elif os.path.isfile(path) and os.path.exists(path):
        sql_files = [path]
    else:
        click.echo(f"Invalid path {path}", err=True)

    # todo: pass
