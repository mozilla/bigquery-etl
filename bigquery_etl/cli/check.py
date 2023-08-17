"""bigquery-etl CLI check command."""
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from subprocess import CalledProcessError
from typing import List, Optional, Union

import click
import sqlparse

from bigquery_etl.format_sql.formatter import reformat

from ..cli.utils import (
    is_authenticated,
    paths_matching_checks_pattern,
    project_id_option,
    sql_dir_option,
)
from ..util.common import render as render_template


def _build_jinja_parameters(query_args):
    """Convert the bqetl parameters to a dictionary for use by the Jinja template."""
    parameters = {}
    for query_arg in query_args:
        param_and_value = query_arg.split("=")
        if len(param_and_value) == 2:
            # e.g. --parameter=download_date:DATE:2023-05-28
            # the dict result is {"download_date": "2023-05-28"}
            bq_parameter = param_and_value[1].split(":")
            if len(bq_parameter) == 3:
                if re.match(r"^\w+$", bq_parameter[0]):
                    parameters[bq_parameter[0]] = bq_parameter[2]
            else:
                # e.g. --project_id=moz-fx-data-marketing-prod
                # the dict result is {"project_id": "moz-fx-data-marketing-prod"}
                if param_and_value[0].startswith("--"):
                    parameters[param_and_value[0].strip("--")] = param_and_value[1]
        else:
            if query_arg == "--dry_run":
                continue

            print(f"parameter {query_arg} will not be used to render Jinja template.")
    return parameters


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
    Renders data check query using parameters provided (OPTIONAL).
    \b
    The result is what would be used to run a check to ensure that the specified dataset
    adheres to the assumptions defined in the corresponding checks.sql file

    Example:

    \t./bqetl check render --project_id=moz-fx-data-marketing-prod ga_derived.downloads_with_attribution_v2 --parameter=download_date:DATE:2023-05-01
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("dataset")
@project_id_option()
@sql_dir_option
@click.pass_context
def render(
    ctx: click.Context, dataset: str, project_id: Optional[str], sql_dir: Optional[str]
) -> None:
    """Render a check's Jinja template."""
    checks_file, project_id, dataset_id, table = paths_matching_checks_pattern(
        dataset, sql_dir, project_id=project_id
    )

    click.echo(
        _render(
            checks_file,
            dataset_id,
            table,
            project_id=project_id,
            query_arguments=ctx.args[:],
        )
    )

    return None


def _render(
    checks_file: Path,
    dataset_id: str,
    table: str,
    project_id: Union[str, None] = None,
    query_arguments: List[str] = list(),
):
    if checks_file is None:
        return

    checks_file = Path(checks_file)

    query_arguments.append("--use_legacy_sql=false")

    if project_id is not None:
        query_arguments.append(f"--project_id={project_id}")

    # Convert all the Airflow params to jinja usable dict.
    parameters = _build_jinja_parameters(query_arguments)

    jinja_params = {
        **{"dataset_id": dataset_id, "table_name": table},
        **parameters,
    }

    rendered_check_query = render_template(
        checks_file.name,
        template_folder=str(checks_file.parent),
        templates_dir="",
        format=False,
        **jinja_params,
    )

    # replace query @params with param values passed via the cli
    for param, value in parameters.items():
        if param in rendered_check_query:
            rendered_check_query = rendered_check_query.replace(
                f"@{param}", f'"{value}"'
            )

    rendered_check_query = reformat(rendered_check_query)

    return rendered_check_query


@check.command(
    help="""
    Runs data checks defined for the dataset (checks.sql).

    Example:

    \t./bqetl check run ga_derived.downloads_with_attribution_v2 --parameter=download_date:DATE:2023-05-01

    Checks can be validated using the `--dry_run` flag without executing them:

    \t./bqetl check run --dry_run ga_derived.downloads_with_attribution_v2 --parameter=download_date:DATE:2023-05-01
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("dataset")
@project_id_option()
@sql_dir_option
@click.option(
    "--dry_run",
    "--dry-run",
    is_flag=True,
    default=False,
    help="To dry run the query to make sure it is valid",
)
@click.pass_context
def run(ctx, dataset, project_id, sql_dir, dry_run):
    """Run a check."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
            "and check that the project is set correctly."
        )
        sys.exit(1)

    checks_file, project_id, dataset_id, table = paths_matching_checks_pattern(
        dataset, sql_dir, project_id=project_id
    )

    _run_check(
        checks_file,
        project_id,
        dataset_id,
        table,
        ctx.args,
        dry_run=dry_run,
    )


def _run_check(
    checks_file,
    project_id,
    dataset_id,
    table,
    query_arguments,
    dry_run=False,
):
    """Run the check."""
    if checks_file is None:
        return

    checks_file = Path(checks_file)

    query_arguments.append("--use_legacy_sql=false")
    if project_id is not None:
        query_arguments.append(f"--project_id={project_id}")

    if dry_run is True:
        query_arguments.append("--dry_run")

    # Convert all the Airflow params to jinja usable dict.
    parameters = _build_jinja_parameters(query_arguments)

    jinja_params = {
        **{"dataset_id": dataset_id, "table_name": table},
        **parameters,
    }

    rendered_result = render_template(
        checks_file.name,
        template_folder=str(checks_file.parent),
        templates_dir="",
        format=False,
        **jinja_params,
    )

    checks = sqlparse.split(rendered_result)
    seek_location = 0
    check_failed = False

    with tempfile.NamedTemporaryFile(mode="w+") as query_stream:
        for rendered_check in checks:
            # since the last check will end with ; the last entry will be empty string.
            if len(rendered_check) == 0:
                continue
            rendered_check = rendered_check.strip()
            query_stream.write(rendered_check)
            query_stream.seek(seek_location)
            seek_location += len(rendered_check)

            # run the query as shell command so that passed parameters can be used as is
            try:
                subprocess.check_output(
                    ["bq", "query"] + query_arguments,
                    stdin=query_stream,
                    encoding="UTF-8",
                )
            except CalledProcessError as e:
                print(_parse_check_output(e.output))
                check_failed = True

    if check_failed:
        sys.exit(1)
