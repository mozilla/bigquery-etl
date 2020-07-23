import click


def create_query_cli_group():
    """Create the CLI group for the query command."""
    group = click.Group(name="query")
    group.add_command(query_create_command)
    return group


@click.command(name="create", help="Create a new query.")
def query_create_command():
    """CLI command for creating a new query."""
    print("todo create new query")


query_cli = create_query_cli_group()
