"""Generate clients city seen per app.  This code is only used to initialize a derived table for each app."""

import os
from enum import Enum
from pathlib import Path
from typing import List

import click
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import render, write_sql

THIS_PATH = Path(os.path.dirname(__file__))
TABLE_NAME = os.path.basename(os.path.normpath(THIS_PATH))

HEADER = (
    "-- Query generated via sql_generators.clients_city_seen.\n"
    "-- this mimics the logic used in baseline_clients_daily_v1.\n"
    "-- some client_id do not have first_seen_geo_date since stable tables only have 2 years of data.\n"
    "{% if is_init() %}"
)
FOOTER = "{% else %}\n" "{% endif %}"

LINES_TO_STRIP = {"{% raw %}", "{% endraw %}", "WITH"}

FINAL_UNION = (
    "SELECT\n"
    "'{app_name}' AS app_name,\n"
    "  *\n"
    "FROM\n"
    "  clients_city_first_seen_{app_name} AS fs\n"
    "FULL JOIN\n"
    "  clients_city_last_seen_{app_name} AS ls\n"
    "USING (client_id)"
)


def clean(s: str) -> str:
    """Clean a generated SQL statement, so it can be merged using UNION ALL."""
    lines_to_remove = {
        ln.strip() for ln in (HEADER + "\n" + FOOTER).splitlines() if ln.strip()
    } | LINES_TO_STRIP

    kept = [ln for ln in s.splitlines() if ln.strip() not in lines_to_remove]
    s = "\n".join(kept).strip().rstrip(" ,;")

    lines = s.splitlines()
    if len(lines) >= 9:
        s = "\n".join(lines[:-9])

    return s


def union_with_single_init(statements: List[str], app_names: List[str]) -> str:
    """
    Strip the given header/footer lines from each fragment.

    UNION ALL the bodies, then wrap the whole result once with header+footer.
    """
    parts = "\nUNION ALL\n".join(
        FINAL_UNION.format(app_name=a.strip()) for a in app_names if a and a.strip()
    ).rstrip("\n")

    bodies = ",\n".join(
        b for b in (clean(s) for s in statements if s and s.strip()) if b
    )

    with_block = f"WITH\n{bodies}\n" if bodies else ""
    return f"{HEADER}\n{with_block}{parts}\n{FOOTER}"


class Browsers(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    firefox_desktop = "Firefox Desktop"
    fenix = "Fenix"
    # focus_ios = "Focus iOS"
    # focus_android = "Focus Android"
    # firefox_ios = "Firefox iOS"
    # klar_ios = "Klar iOS"
    # klar_android = "Klar Android"


@click.command()
@click.option(
    "--output-dir",
    "--output_dir",
    help="Output directory generated SQL is written to",
    type=click.Path(file_okay=False),
    default="sql",
)
@click.option(
    "--target-project",
    "--target_project",
    help="Google Cloud project ID",
    default="moz-fx-data-shared-prod",
)
@use_cloud_function_option
def generate(target_project, output_dir, use_cloud_function):
    """Generate per-app queries, schemas, and metadata for clients’ first/last seen city and subdivisions.

    Initializes the derived tables from stable sources.
    After the city/subdivision fields in the stable tables are nulled,
    this initializer is no longer applicable — ongoing updates will come from the live tables.
    """
    env = Environment(loader=FileSystemLoader(str(THIS_PATH / "templates")))
    output_dir = Path(output_dir) / target_project
    # query templates
    query_template = env.get_template("query.sql")
    # schema templates
    schema_template = "schema.yaml"
    # metadata template
    metadata_template = "metadata.yaml"

    for browser in Browsers:

        if browser.name == "fenix":

            app_id_list = [
                "org_mozilla_firefox",
                "org_mozilla_fenix_nightly",
                "org_mozilla_fennec_aurora",
                "org_mozilla_firefox_beta",
                "org_mozilla_fenix",
            ]

            firefox_android_queries = [
                query_template.render(
                    project_id=target_project,
                    app_name=app_id,
                )
                for app_id in app_id_list
            ]

            query_sql = union_with_single_init(firefox_android_queries, app_id_list)

        else:
            query_sql = reformat(
                query_template.render(
                    project_id=target_project,
                    app_name=browser.name,
                )
            )

        # Write query SQL files.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="query.sql",
            sql=query_sql,
            skip_existing=False,
        )

        # Write metadata YAML files.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="metadata.yaml",
            sql=render(
                metadata_template,
                template_folder=THIS_PATH / "templates",
                app_name=browser.name,
                table_name=TABLE_NAME,
                format=False,
            ),
            skip_existing=False,
        )

        # Write schema YAML files.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="schema.yaml",
            sql=render(
                schema_template,
                template_folder=THIS_PATH / "templates",
                format=False,
            ),
            skip_existing=False,
        )
