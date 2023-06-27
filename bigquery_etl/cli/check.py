"""bigquery-etl CLI check command."""
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from subprocess import CalledProcessError

import click
import sqlparse

from ..cli.utils import (
    is_authenticated,
    paths_matching_checks_pattern,
    project_id_option,
    sql_dir_option,
)
from ..util.common import render as render_template

ROOT = Path(__file__).parent.parent.parent
CHECKS_MACROS_DIR = ROOT / "tests" / "checks"


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
    Run ETL checks.
s    \b

    Example:
     ./bqetl check run ga_derived.downloads_with_attribution_v2 --parameter=download_date:DATE:2023-05-01
    """,
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.argument("name")
@project_id_option()
@sql_dir_option
@click.pass_context
def run(ctx, name, project_id, sql_dir):
    """Run a check."""
    if not is_authenticated():
        click.echo(
            "Authentication to GCP required. Run `gcloud auth login` "
            "and check that the project is set correctly."
        )
        sys.exit(1)

    checks_file, project_id, dataset_id, table = paths_matching_checks_pattern(
        name, sql_dir, project_id=project_id
    )

    _run_check(
        checks_file,
        project_id,
        dataset_id,
        table,
        ctx.args,
    )


def _run_check(
    checks_file,
    project_id,
    dataset_id,
    table,
    query_arguments,
):
    """Run the check."""
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

    # make macros available by creating a temporary file and pasting macro definition
    # alongside checks.
    # it is not possible to use `include` or `import` since the macros live in a different
    # directory than the checks Jinja template.
    with tempfile.NamedTemporaryFile(mode="w+") as checks_template:
        macro_imports = "\n".join(
            [macro_file.read_text() for macro_file in CHECKS_MACROS_DIR.glob("*")]
        )
        checks_template = Path(checks_template.name)
        checks_template.write_text(macro_imports + "\n" + checks_file.read_text())

        rendered_result = render_template(
            checks_template.name,
            template_folder=str(checks_template.parent),
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
