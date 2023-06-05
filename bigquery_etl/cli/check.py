"""bigquery-etl CLI check command."""
import datetime
import re
import subprocess
import sys
import tempfile
from subprocess import CalledProcessError

import click

from ..cli.utils import is_authenticated, paths_matching_checks_pattern, sql_dir_option
from ..util.common import render as render_template


def _parse_partition_setting(partition_date):
    params = partition_date.split(":")
    if len(params) != 3:
        return None

    # Check date format
    try:
        datetime.datetime.strptime(params[2], "%Y-%m-%d").date()
    except ValueError:
        return None

    # Check column name
    if re.match(r"^\w+$", params[0]):
        return {params[0]: params[2]}


def _validate_partition_date(ctx, param, partition_date):
    """Process the CLI parameter check_date and set the parameter for BigQuery."""
    # Will be None if launched from Airflow.  Also ctx.args is not populated at this stage.
    if partition_date:
        parsed = _parse_partition_setting(partition_date)
        if parsed is None:
            raise click.BadParameter("Format must be <column-name>::<yyyy-mm-dd>")
        return parsed
    return None


def _parse_check_output(output: str) -> str:
    output = output.replace("\n", " ")
    if "ETL Data Check Failed:" in output:
        return f"ETL Data Check Failed:{output.split('ETL Data Check Failed:')[1]}"
    return output


@click.group(
    help="""
        Commands for managing data checks.
        \b

        UNDER ACTIVE DEVELOPMENT See https://mozilla-hub.atlassian.net/browse/DENG-919
        """
)
@click.pass_context
def check(ctx):
    """Create the CLI group for the check command."""
    # create temporary directory generated content is written to
    # the directory will be deleted automatically after the command exits
    ctx.ensure_object(dict)
    ctx.obj["TMP_DIR"] = ctx.with_resource(tempfile.TemporaryDirectory())


@check.command(
    help="""
    Run ETL checks.
s    \b

    Example:
     ./bqetl check run ga_derived.downloads_with_attribution_v2 --partition download_date::2023-05-01
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("name")
@sql_dir_option
@click.option(
    "--partition",
    "-p",
    help="Partition to check, format <column-name>::<yyy-mm-dd>, must be provided if not executing in Airflow ",
    type=click.UNPROCESSED,
    callback=_validate_partition_date,
    required=False,
)
@click.pass_context
def run(ctx, name, sql_dir, partition):
    """Run a check."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
            "and check that the project is set correctly."
        )
        sys.exit(1)

    checks_file, project_id, dataset_id, table = paths_matching_checks_pattern(
        name, sql_dir, project_id=None
    )

    _check_query(
        checks_file,
        project_id,
        dataset_id,
        table,
        partition,
        ctx.args,
    )


def _check_query(
    checks_file,
    project_id,
    dataset_id,
    table,
    partition,
    query_arguments,
):
    """Run the check."""
    query_arguments.append("--use_legacy_sql=false")
    if project_id is not None:
        query_arguments.append(f"--project_id={project_id}")

    # Partition will be None if from Airflow
    if partition is None:
        # Need to check the query_arguments for the value
        for parameter in query_arguments:
            if parameter.startswith("--parameter"):
                param_value = parameter.split("=")[1]
                partition = _parse_partition_setting(param_value)
                # once we have a value that passed the date check stop checking.
                if partition is not None:
                    break
    else:
        # We have a partition value from the CLI so add to the query_arguments.
        # There should only be 1
        key, value = next(iter(partition.items()))
        query_arguments.append(f"--parameter={key}::{value}")

    if partition is None:
        raise ValueError("No partition specified to check.")

    jinja_params = {
        **{"project_id": project_id, "dataset_id": dataset_id, "table_name": table},
        **partition,
    }

    with tempfile.NamedTemporaryFile(mode="w+") as query_stream:
        query_stream.write(
            render_template(
                checks_file.name,
                template_folder=str(checks_file.parent),
                templates_dir="",
                format=False,
                **jinja_params,
            )
        )
        query_stream.seek(0)

        # run the query as shell command so that passed parameters can be used as is
        try:
            subprocess.check_output(
                ["bq", "query"] + query_arguments, stdin=query_stream, encoding="UTF-8"
            )
        except CalledProcessError as e:
            print(_parse_check_output(e.output))
            sys.exit(1)
