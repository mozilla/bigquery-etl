"""bigquery-etl CLI dag command."""

import click
import os
import sys
import yaml

from ..query_scheduling.dag_collection import DagCollection
from ..query_scheduling.dag import Dag
from ..query_scheduling.generate_airflow_dags import get_dags


@click.group()
def dag():
    """Create the CLI group for the dag command."""
    pass


@dag.command(help="List all available DAGs",)
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
    help="Path to directory with queries",
    type=click.Path(file_okay=False),
    default="sql/",
)
@click.option(
    "--with_tasks",
    "--with-tasks",
    "-t",
    help="Include scheduled tasks",
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


@dag.command(
    help="Create a new DAG with name bqetl_<dag_name>, for example: bqetl_search"
)
@click.argument("name")
@click.option(
    "--dags_config",
    "--dags-config",
    help="Path to dags.yaml config file",
    type=click.Path(file_okay=True),
    default="dags.yaml",
)
@click.option(
    "--schedule_interval",
    "--schedule-interval",
    help=(
        "Schedule interval of the new DAG. "
        "Schedule intervals can be either in CRON format or one of: "
        "@once, @hourly, @daily, @weekly, @monthly, @yearly or a timedelta []d[]h[]m"
    ),
    required=True,
)
@click.option(
    "--owner", help=("Email address of the DAG owner"), required=True,
)
@click.option(
    "--start_date",
    "--start-date",
    help=("First date for which scheduled queries should be executed"),
    required=True,
)
@click.option(
    "--email",
    help=("List of email addresses that Airflow will send alerts to"),
    default=["telemetry-alerts@mozilla.com"],
)
@click.option(
    "--retries",
    help=("Number of retries Airflow will attempt in case of failures"),
    default=2,
)
@click.option(
    "--retry_delay",
    "--retry-delay",
    help=(
        "Time period Airflow will wait after failures before running failed tasks again"
    ),
    default="30m",
)
def create(
    name, dags_config, schedule_interval, owner, start_date, email, retries, retry_delay
):
    """Create a new DAG."""
    if not os.path.isfile(dags_config):
        click.echo(f"Invalid DAG config file: {dags_config}.", err=True)
        sys.exit(1)

    # create a DAG and validate all properties
    new_dag = Dag.from_dict(
        {
            name: {
                "schedule_interval": schedule_interval,
                "default_args": {
                    "owner": owner,
                    "start_date": start_date,
                    "email": set(email + [owner]),
                    "retries": retries,
                    "retry_delay": retry_delay,
                },
            }
        }
    )

    with open(dags_config, "a") as dags_file:
        dags_file.write("\n")
        dags_file.write(yaml.dump(new_dag.to_dict()))
        dags_file.write("\n")

    click.echo(f"Added new DAG definition to {dags_config}")
