"""Generate active users aggregates per app."""

import os
from enum import Enum
from pathlib import Path

import click
import yaml
from jinja2 import Environment, FileSystemLoader

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.util.common import render, write_sql, get_table_dir

THIS_PATH = Path(os.path.dirname(__file__))
MOBILE_UNITTESTS_PATH = THIS_PATH / "templates/unittests/mobile"
DESKTOP_UNITTESTS_PATH = THIS_PATH / "templates/unittests/desktop"
MOBILE_UNITTESTS_TEMPLATES = THIS_PATH / "templates" / "mobile_unittests_templates.yaml"
DESKTOP_UNITTESTS_TEMPLATES = (
    THIS_PATH / "templates" / "desktop_unittests_templates.yaml"
)

TABLE_NAME = os.path.basename(os.path.normpath(THIS_PATH))
BASE_NAME = "_".join(TABLE_NAME.split("_")[:-1])
DATASET_FOR_UNIONED_VIEWS = "telemetry"
CHECKS_TEMPLATE_CHANNELS = {
    "firefox_ios": [
        {
            "name": "release",
            "table": "`moz-fx-data-shared-prod.org_mozilla_ios_firefox_live.baseline_v1`",
        },
        {
            "name": "beta",
            "table": "`moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_live.baseline_v1`",
        },
        {
            "name": "nightly",
            "table": "`moz-fx-data-shared-prod.org_mozilla_ios_fennec_live.baseline_v1`",
        },
    ],
    "focus_ios": [
        {"table": "`moz-fx-data-shared-prod.org_mozilla_ios_focus_live.baseline_v1`"}
    ],
    "focus_android": [
        {
            "name": "release",
            "table": "`moz-fx-data-shared-prod.org_mozilla_focus_live.baseline_v1`",
        },
        {
            "name": "beta",
            "table": "`moz-fx-data-shared-prod.org_mozilla_focus_beta_live.baseline_v1`",
        },
        {
            "name": "nightly",
            "table": "`moz-fx-data-shared-prod.org_mozilla_focus_nightly_live.baseline_v1`",
        },
    ],
    "klar_ios": [
        {"table": "`moz-fx-data-shared-prod.org_mozilla_ios_klar_live.baseline_v1`"}
    ],
    "klar_android": [
        {"table": "`moz-fx-data-shared-prod.org_mozilla_klar_live.baseline_v1`"}
    ],
}


class Browsers(Enum):
    """Enumeration with browser names and equivalent dataset names."""

    firefox_desktop = "Firefox Desktop"


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
    """Generate per-app queries, views and metadata for active users and search counts aggregates.

    The parent folders will be created if not existing and existing files will be overwritten.
    """
    env = Environment(loader=FileSystemLoader(str(THIS_PATH / "templates")))
    output_dir = Path(output_dir) / target_project
    # query templates
    mobile_query_template = env.get_template("mobile_query.sql")
    desktop_query_template = env.get_template("desktop_query.sql")
    focus_android_query_template = env.get_template("focus_android_query.sql")
    # view templates
    focus_android_view_template = env.get_template("focus_android_view.sql")
    mobile_view_template = env.get_template("mobile_view.sql")
    view_template = env.get_template("view.sql")
    # metadata template
    metadata_template = "metadata.yaml"
    # schema templates
    desktop_table_schema_template = "desktop_table_schema.yaml"
    desktop_view_schema_template = "desktop_view_schema.yaml"
    mobile_table_schema_template = "mobile_table_schema.yaml"
    mobile_view_schema_template = "mobile_view_schema.yaml"
    # backfill template
    desktop_backfill_template = "desktop_backfill.yaml"
    # checks templates
    desktop_checks_template = env.get_template("desktop_checks.sql")
    fenix_checks_template = env.get_template("fenix_checks.sql")
    mobile_checks_template = env.get_template("mobile_checks.sql")
    # checks templates for BigEye
    bigeye_checks_template = env.get_template("bigconfig.yml")

    def output_unittest_templates(
        dataset, app_name, templates, template_type, source, test_name=None
    ):
        """Load, render and output unitest template."""
        test_folder = ""
        parts = 2
        # Handle absolute and relative paths for tests
        output_path = Path("tests").joinpath(*output_dir.parts[output_dir.is_absolute():])

        for group in templates[template_type]:
            try:
                for file_name, file_template in group[source].items():
                    if template_type == "test_data":
                        parts = 3
                        test_folder = (
                            "/desktop" if dataset == "firefox_desktop" else "/mobile"
                        ) + f"/{test_name}"
                    _dataset = (
                        "telemetry"
                        if (
                            dataset == "firefox_desktop"
                            and file_name == "desktop_active_users"
                        )
                        else dataset
                    )
                    _file = (
                        file_template
                        if file_name in ["expect", "query_params"]
                        else f"{target_project}.{_dataset}.{file_template}"
                    )

                    unittest_template = env.get_template(
                        f"unittests{test_folder}/{file_template}"
                    )
                    full_table_id = (
                        f"{target_project}.{dataset}_derived.{TABLE_NAME}"
                        + (f".{test_name}" if template_type == "test_data" else "")
                    )
                    d = get_table_dir(output_path, full_table_id, parts)
                    d.mkdir(parents=True, exist_ok=True)
                    target = d / _file
                    click.echo(f"INFO:Writing {target}")
                    with target.open("w") as f:
                        f.write(
                            unittest_template.render(
                                app_name=app_name,
                                format=False,
                            )
                        )
                        f.write("\n")
            except KeyError:
                continue

    for browser in Browsers:
        # Load list of unittests schemas and data templates.
        templates_yaml_path = (
            DESKTOP_UNITTESTS_TEMPLATES
            if browser.name == "firefox_desktop"
            else MOBILE_UNITTESTS_TEMPLATES
        )
        tests_path = (
            DESKTOP_UNITTESTS_PATH
            if browser.name == "firefox_desktop"
            else MOBILE_UNITTESTS_PATH
        )

        with open(templates_yaml_path) as f:
            templates = yaml.safe_load(f)

        # Write unittests test schemas.
        output_unittest_templates(
            dataset=browser.name,
            app_name=browser.value,
            templates=templates,
            template_type="test_schemas",
            source="common",
        )
        output_unittest_templates(
            dataset=browser.name,
            app_name=browser.value,
            templates=templates,
            template_type="test_schemas",
            source=browser.name,
        )

        # Write unittests test data.
        for test in next(os.walk(tests_path))[1]:
            output_unittest_templates(
                dataset=browser.name,
                app_name=browser.value,
                templates=templates,
                template_type="test_data",
                source="common",
                test_name=test,
            )
            output_unittest_templates(
                dataset=browser.name,
                app_name=browser.value,
                templates=templates,
                template_type="test_data",
                source=browser.name,
                test_name=test,
            )

        # Write queries.
        if browser.name == "firefox_desktop":
            query_sql = reformat(
                desktop_query_template.render(
                    app_value=browser.value,
                )
            )
            table_schema_template = desktop_table_schema_template
            view_schema_template = desktop_view_schema_template
        elif browser.name == "focus_android":
            query_sql = reformat(
                focus_android_query_template.render(
                    project_id=target_project,
                    app_name=browser.name,
                )
            )
            table_schema_template = mobile_table_schema_template
            view_schema_template = mobile_view_schema_template
        else:
            query_sql = reformat(
                mobile_query_template.render(
                    project_id=target_project,
                    app_value=browser.value,
                    app_name=browser.name,
                )
            )
            table_schema_template = mobile_table_schema_template
            view_schema_template = mobile_view_schema_template

        # Create checks_sql
        if browser.name == "firefox_desktop":
            checks_sql = desktop_checks_template.render(
                project_id=target_project,
                app_value=browser.value,
                app_name=browser.name,
            )
        elif browser.name == "fenix":
            checks_sql = fenix_checks_template.render(
                project_id=target_project,
                app_value=browser.value,
                app_name=browser.name,
            )
        elif browser.name in CHECKS_TEMPLATE_CHANNELS.keys():
            checks_sql = mobile_checks_template.render(
                project_id=target_project,
                app_value=browser.value,
                app_name=browser.name,
                channels=CHECKS_TEMPLATE_CHANNELS[browser.name],
            )

        # Write table schema YAML.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="schema.yaml",
            sql=render(
                table_schema_template,
                template_folder=THIS_PATH / "templates",
                format=False,
            ),
            skip_existing=False,
        )

        # Write view schema YAML.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}.{BASE_NAME}",
            basename="schema.yaml",
            sql=render(
                view_schema_template,
                template_folder=THIS_PATH / "templates",
                format=False,
            ),
            skip_existing=False,
        )

        # Write query SQL.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="query.sql",
            sql=query_sql,
            skip_existing=False,
        )

        # Write view SQL.
        if browser.name == "focus_android":
            write_sql(
                output_dir=output_dir,
                full_table_id=f"{target_project}.{browser.name}.{BASE_NAME}",
                basename="view.sql",
                sql=reformat(
                    focus_android_view_template.render(
                        project_id=target_project,
                        app_name=browser.name,
                        table_name=TABLE_NAME,
                    )
                ),
                skip_existing=False,
            )
        else:
            write_sql(
                output_dir=output_dir,
                full_table_id=f"{target_project}.{browser.name}.{BASE_NAME}",
                basename="view.sql",
                sql=reformat(
                    view_template.render(
                        project_id=target_project,
                        app_name=browser.name,
                        table_name=TABLE_NAME,
                    )
                ),
                skip_existing=False,
            )

        # Write metadata YAML.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="metadata.yaml",
            sql=render(
                metadata_template,
                template_folder=THIS_PATH / "templates",
                app_value=browser.value,
                app_name=browser.name,
                table_name=TABLE_NAME,
                format=False,
            ),
            skip_existing=False,
        )

        # Write checks SQL.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="checks.sql",
            sql=checks_sql,
            skip_existing=False,
        )

        # Write BigEye Config YAML.
        write_sql(
            output_dir=output_dir,
            full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
            basename="bigconfig.yml",
            sql=bigeye_checks_template.render(
                app_name=browser.name,
                format=False,
            ),
            skip_existing=False,
        )

        # Write backfill YAML.
        if browser.name == "firefox_desktop":
            write_sql(
                output_dir=output_dir,
                full_table_id=f"{target_project}.{browser.name}_derived.{TABLE_NAME}",
                basename="backfill.yaml",
                sql=render(
                    desktop_backfill_template,
                    template_folder=THIS_PATH / "templates",
                    format=False,
                ),
                skip_existing=False,
            )
