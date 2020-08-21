"""bigquery-etl CLI dag command."""

import click
import os
import sys

from ..query_scheduling.dag_collection import DagCollection
from ..query_scheduling.generate_airflow_dags import get_dags


@click.group()
def dag():
    """Create the CLI group for the dag command."""
    pass


@dag.command(help="List all available DAGs.",)
@click.option(
    "--dags_config",
    "--dags-config",
    help="Path to dags.yaml config file",
    type=click.Path(file_okay=True),
    default="dags.yaml",
)
@click.option(
    "--sql-dir",
    "-sql_dir",
    help="Path to directory with queries.",
    type=click.Path(file_okay=False),
    default="sql/",
)
@click.option(
    "--with_tasks",
    "--with-tasks",
    "-t",
    help="Include scheduled tasks.",
    default=False,
    is_flag=True,
)
def list(dags_config, sql_dir, with_tasks):
    """List all available DAGs."""
    if not os.path.isfile(dags_config):
        click.echo(f"Invalid DAG config file: {dags_config}.", err=True)
        sys.exit(1)

    if with_tasks:
        if not os.path.isdir(sql_dir):
            click.echo(f"Invalid path to query files: {sql_dir}.", err=True)
            sys.exit(1)

        dag_collection = get_dags(sql_dir, dags_config)
    else:
        dag_collection = DagCollection.from_file(dags_config)

    sorted_dags = sorted(dag_collection.dags, key=lambda d: d.name)

    for dag in sorted_dags:
        click.secho(click.style(dag.name, bold=True))
        click.echo(f"schedule_interval: {dag.schedule_interval}")
        click.echo(f"owner: {dag.default_args.owner}")

        if with_tasks:
            click.echo("tasks: ")
            for task in sorted(dag.tasks, key=lambda d: d.table):
                click.echo(f"  - {task.dataset}.{task.table}_{task.version}")

        click.echo("")
