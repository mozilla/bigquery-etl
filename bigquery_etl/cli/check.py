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


def _build_query_arguments(parameters):
    """
    Convert a dict of parameters (from CLI) into query arguments.

    Opposite of _build_query_arguments.
    """
    if parameters is None:
        return []
    return [f"--parameter={key}::{value}" for key, value in parameters.items()]


def _parse_query_arguments(query_args):
    parameters = []
    for query_arg in query_args:
        if query_arg.startswith("--parameter=") and len(query_arg.split("=")) == 2:
            parameters.append(query_arg.split("=")[1])
        else:
            raise ValueError("argument must start with --parameter")
    return _build_parameters(None, None, parameters)


def _build_parameters(ctx=None, param=None, parameters=None):
    """
    Convert the list of parameters from format <param-name>::<param-value> to dict.

    If an invalid parameter is found, empty dict is returned.  Opposite of _build_query_arguments.
    """
    result = {}
    detected_partition = False

    if parameters is None:
        return {}

    for param in parameters:
        params = param.split(":")
        if len(params) != 3:
            raise ValueError(
                f"parameter: {param} is not a valid parameter.  Please use format: <param-name>::<param-value>, exiting"
            )

        # If the param value smells like a date then check if it is a valid date.
        if re.match(r"^[0-9-]+$", params[2]):
            try:
                datetime.datetime.strptime(params[2], "%Y-%m-%d").date()
                detected_partition = True
                print(f"Found date parameter:  {param}")
            except ValueError:
                raise ValueError(
                    f"parameter: {params[0]} with value: {params[2]} is not a valid date, exiting."
                )

        # Check column name
        if re.match(r"^\w+$", params[0]):
            result[params[0]] = params[2]
        else:
            raise ValueError(
                f"parameter: {params[0]} with value: {params[2]} is not a value parameter name."
            )

        if not detected_partition:
            print(
                "WARNING: No date specified for partition, attempting to check entire table"
            )
    return result


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
     ./bqetl check run ga_derived.downloads_with_attribution_v2 --parameter download_date::2023-05-01
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("name")
@sql_dir_option
@click.option(
    "--parameter",
    "-p",
    help="Parameters required for processing including partition. If table is partitioned then the "
    " format <column-name>::<yyy-mm-dd>, must be provided if not executing in Airflow. "
    " Since some tables are not partitioned parameters are not required. ",
    type=click.UNPROCESSED,
    callback=_build_parameters,
    multiple=True,
    required=False,
)
@click.pass_context
def run(ctx, name, sql_dir, parameter):
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

    _run_check(
        checks_file,
        project_id,
        dataset_id,
        table,
        parameter,
        ctx.args,
    )


def _run_check(
    checks_file,
    project_id,
    dataset_id,
    table,
    parameters,
    query_arguments,
):
    """Run the check."""
    query_arguments.append("--use_legacy_sql=false")
    if project_id is not None:
        query_arguments.append(f"--project_id={project_id}")

    if parameters is None:
        # Convert all the Airflow params to jinja usable dict.
        parameters = _parse_query_arguments(query_arguments)
    else:
        # Convert all CLI parameters to BQ query args.
        query_arguments += _build_query_arguments(parameters)

    jinja_params = {
        **{"project_id": project_id, "dataset_id": dataset_id, "table_name": table},
        **parameters,
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
