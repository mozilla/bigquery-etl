"""bigquery-etl CLI dag command."""

import click
import os
from pathlib import Path
import sys
import yaml

from ..query_scheduling.dag_collection import DagCollection
from ..query_scheduling.dag import Dag
from ..query_scheduling.generate_airflow_dags import get_dags
from ..cli.utils import is_valid_dir, is_valid_file
from ..metadata.parse_metadata import Metadata, METADATA_FILE

dags_config_option = click.option(
    "--dags_config",
    "--dags-config",
    help="Path to dags.yaml config file",
    type=click.Path(file_okay=True),
    default="dags.yaml",
    callback=is_valid_file,
)

sql_dir_option = click.option(
    "--sql-dir",
    "--sql_dir",
    help="Path to directory with queries",
    type=click.Path(file_okay=False),
    default="sql/",
    callback=is_valid_dir,
)

output_dir_option = click.option(
    "--output-dir",
    "--output_dir",
    help="Path directory with generated DAGs",
    type=click.Path(file_okay=False),
    default="dags/",
    callback=is_valid_dir,
)


@click.group()
def dag():
    """Create the CLI group for the dag command."""
    pass


@dag.command(help="List all available DAGs",)
@click.argument("name", required=False)
@sql_dir_option
@dags_config_option
@click.option(
    "--with_tasks",
    "--with-tasks",
    "-t",
    help="Include scheduled tasks",
    default=False,
    is_flag=True,
)
def info(name, dags_config, sql_dir, with_tasks):
    """List available DAG information."""
    if with_tasks:
        dag_collection = get_dags(sql_dir, dags_config)
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
                click.echo(f"  - {task.dataset}.{task.table}_{task.version}")

        click.echo("")


@dag.command(
    help="Create a new DAG with name bqetl_<dag_name>, for example: bqetl_search"
)
@click.argument("name")
@dags_config_option
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


@dag.command(help="Generate Airflow DAGs from DAG definitions")
@click.argument("name", required=False)
@dags_config_option
@sql_dir_option
@output_dir_option
def generate(name, dags_config, sql_dir, output_dir):
    """CLI command for generating Airflow DAGs."""
    dags = get_dags(sql_dir, dags_config)
    if name:
        # only generate specific DAG
        dag = dags.dag_by_name(name)

        if not dag:
            click.echo(f"DAG {name} does not exist.", err=True)
            sys.exit(1)

        dags.dag_to_airflow(output_dir, dag)
        click.echo(f"Generated {output_dir}{dag.name}.py")
    else:
        # re-generate all DAGs
        dags.to_airflow_dags(output_dir)
        click.echo("DAG generation complete.")


@dag.command(help="Remove a DAG")
@click.argument("name", required=False)
@dags_config_option
@sql_dir_option
@output_dir_option
def remove(name, dags_config, sql_dir, output_dir):
    """
    CLI command for removing a DAG.

    Also removes scheduling information from queries that were referring to the DAG.
    """
    # remove from task schedulings
    dags = get_dags(sql_dir, dags_config)
    dag_tbr = dags.dag_by_name(name)

    if not dag_tbr:
        click.echo(f"No existing DAG definition for {name}")
        sys.exit(1)

    for task in dag_tbr.tasks:
        metadata = Metadata.of_sql_file(task.query_file)
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
