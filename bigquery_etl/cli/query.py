import click


def create_query_cli_group():
    """Create the CLI group for the query command."""
    group = click.Group(name="query", help="Creating/validating/deleting/... queries")
    group.add_command(query_create_command)
    return group


@click.command(
    name="create",
    help=(
        "Create a new query with name "
        "<dataset>.<query_name>, for example: telemetry_derived.asn_aggregates"
    ),
)
@click.argument("name")
@click.option(
    "--path",
    "-p",
    help="Path to directory in which query should be created",
    default="sql/",
)
def query_create_command(name):
    """CLI command for creating a new query."""
    # create directory for query
    # create default metadata.yaml and insert options
    # create query.sql
    # optionally create init.sql


query_cli = create_query_cli_group()
