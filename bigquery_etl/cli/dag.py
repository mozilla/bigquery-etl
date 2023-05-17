"""bigquery-etl CLI dag command."""

import os
import sys
from pathlib import Path

import click
import yaml

from ..cli.utils import is_valid_dir, is_valid_file
from ..metadata.parse_metadata import METADATA_FILE, Metadata
from ..query_scheduling.dag import Dag
from ..query_scheduling.dag_collection import DagCollection
from ..query_scheduling.generate_airflow_dags import get_dags

dags_config_option = click.option(
    "--dags_config",
    "--dags-config",
    help="Path to dags.yaml config file",
    type=click.Path(file_okay=True),
    default="dags.yaml",
    callback=is_valid_file,
)

output_dir_option = click.option(
    "--output-dir",
    "--output_dir",
    help="Path directory with generated DAGs",
    type=click.Path(file_okay=False),
    default="dags/",
    callback=is_valid_dir,
)


@click.group(help="Commands for managing DAGs.")
def dag():
    """Create the CLI group for the dag command."""
    pass


@dag.command(
    help="""Get information about available DAGs.

    Examples:

    # Get information about all available DAGs
    ./bqetl dag info

    # Get information about a specific DAG
    ./bqetl dag info bqetl_ssl_ratios

    # Get information about a specific DAG including scheduled tasks
    ./bqetl dag info --with_tasks bqetl_ssl_ratios
    """,
)
@click.argument("name", required=False)
@dags_config_option
@click.option(
    "--with_tasks",
    "--with-tasks",
    "-t",
    help="Include scheduled tasks",
    default=False,
    is_flag=True,
)
def info(name, dags_config, with_tasks):
    """List available DAG information."""
    if with_tasks:
        dag_collection = get_dags(None, dags_config)
    else:
        dag_collection = DagCollection.from_file(dags_config)

    if name:
        dag = dag_collection.dag_by_name(name)
        if not dag:
            click.echo(f"DAG {name} does not exist", err=True)
            sys.exit(1)
        sorted_dags = [dag]
    else:
        sorted_dags = sorted(dag_collection.dags, key=lambda d: d.name)

    for dag in sorted_dags:
        click.secho(dag.name, bold=True)
        click.echo(f"schedule_interval: {dag.schedule_interval}")
        click.echo(f"owner: {dag.default_args.owner}")

        if with_tasks:
            click.echo("tasks: ")
            for task in sorted(dag.tasks, key=lambda d: d.table):
                click.echo(
                    f"  - {task.project}.{task.dataset}.{task.table}_{task.version}"
                )

        click.echo("")


@dag.command(
    help="""Create a new DAG with name bqetl_<dag_name>, for example: bqetl_search
    When creating new DAGs, the DAG name must have a `bqetl_` prefix.
    Created DAGs are added to the `dags.yaml` file.

    Examples:

    \b
    ./bqetl dag create bqetl_core \\
    --schedule-interval="0 2 * * *" \\
    --owner=example@mozilla.com \\
    --description="Tables derived from `core` pings sent by mobile applications." \\
    --tag=impact/tier_1 \\
    --start-date=2019-07-25

    \b
    # Create DAG and overwrite default settings
    ./bqetl dag create bqetl_ssl_ratios --schedule-interval="0 2 * * *" \\
    --owner=example@mozilla.com \\
    --description="The DAG schedules SSL ratios queries." \\
    --tag=impact/tier_1 \\
    --start-date=2019-07-20 \\
    --email=example2@mozilla.com \\
    --email=example3@mozilla.com \\
    --retries=2 \\
    --retry_delay=30m
    """
)
@click.argument("name")
@dags_config_option
@click.option(
    "--schedule_interval",
    "--schedule-interval",
    help=(
        "Schedule interval of the new DAG. "
        "Schedule intervals can be either in CRON format or one of: "
        "once, hourly, daily, weekly, monthly, yearly or a timedelta []d[]h[]m"
    ),
    required=True,
)
@click.option(
    "--owner",
    help=("Email address of the DAG owner"),
    required=True,
)
@click.option(
    "--description",
    help=("Description for DAG"),
    required=True,
)
@click.option(
    "--tag",
    help=("Tag to apply to the DAG"),
    required=True,
    multiple=True,
)
@click.option(
    "--start_date",
    "--start-date",
    help=("First date for which scheduled queries should be executed"),
    required=True,
)
@click.option(
    "--email",
    help=("Email addresses that Airflow will send alerts to"),
    default=["telemetry-alerts@mozilla.com"],
    multiple=True,
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
    name,
    dags_config,
    schedule_interval,
    owner,
    description,
    tag,
    start_date,
    email,
    retries,
    retry_delay,
):
    """Create a new DAG."""
    # create a DAG and validate all properties
    new_dag = Dag.from_dict(
        {
            name: {
                "description": description,
                "schedule_interval": schedule_interval,
                "default_args": {
                    "owner": owner,
                    "start_date": start_date,
                    "email": (*email, owner),
                    "retries": retries,
                    "retry_delay": retry_delay,
                },
                "tags": tag,
            }
        }
    )

    with open(dags_config, "a") as dags_file:
        dags_file.write("\n")
        dags_file.write(yaml.dump(new_dag.to_dict()))
        dags_file.write("\n")

    click.echo(f"Added new DAG definition to {dags_config}")


@dag.command(
    help="""Generate Airflow DAGs from DAG definitions.

    Examples:

    # Generate all DAGs
    ./bqetl dag generate

    # Generate a specific DAG
    ./bqetl dag generate bqetl_ssl_ratios
    """
)
@click.argument("name", required=False)
@dags_config_option
@output_dir_option
def generate(name, dags_config, output_dir):
    """CLI command for generating Airflow DAGs."""
    dags = get_dags(None, dags_config)
    if name:
        # only generate specific DAG
        dag = dags.dag_by_name(name)

        if not dag:
            click.echo(f"DAG {name} does not exist.", err=True)
            sys.exit(1)

        dags.to_airflow_dags(output_dir, dag_to_generate=dag)
        click.echo(f"Generated {output_dir}{dag.name}.py")
    else:
        # re-generate all DAGs
        dags.to_airflow_dags(output_dir)
        click.echo("DAG generation complete.")


@dag.command(
    help="""Remove a DAG.
    This will also remove the scheduling information from the queries that were scheduled
    as part of the DAG.

    Examples:

    # Remove a specific DAG
    ./bqetl dag remove bqetl_vrbrowser
    """
)
@click.argument("name", required=False)
@dags_config_option
@output_dir_option
def remove(name, dags_config, output_dir):
    """
    CLI command for removing a DAG.

    Also removes scheduling information from queries that were referring to the DAG.
    """
    # remove from task schedulings
    dags = get_dags(None, dags_config)
    dag_tbr = dags.dag_by_name(name)

    if not dag_tbr:
        click.echo(f"No existing DAG definition for {name}")
        sys.exit(1)

    for task in dag_tbr.tasks:
        metadata = Metadata.of_query_file(task.query_file)
        sql_path = Path(os.path.dirname(task.query_file))
        metadata.scheduling = {}
        metadata_file = sql_path / METADATA_FILE
        metadata.write(metadata_file)

    # remove from dags.yaml
    with open(dags_config) as dags_file:
        dags_config_dict = yaml.full_load(dags_file)
        del dags_config_dict[name]

        with open(dags_config, "w") as dags_file:
            dags_file.write(yaml.dump(dags_config_dict))

    output_dir = Path(output_dir)
    # delete generated DAG from dags/
    if os.path.exists(output_dir / (name + ".py")):
        os.remove(output_dir / (name + ".py"))

    click.echo(f"DAG {name} and referenced removed.")
