"""
Generate user-facing views on top of stable tables.

If there are multiple versions of a given document type, the generated view
references only the most recent.

Note that no view will be generated if a corresponding view is already
present in the target directory, which allows manual overrides of views by
checking them into the sql/ tree of the default branch of the repository.
"""

import logging
from functools import partial
from itertools import groupby
from pathlib import Path

import click
from pathos.multiprocessing import ProcessingPool

from bigquery_etl.schema.stable_table_schema import SchemaFile, get_stable_table_schemas

VIEW_QUERY_TEMPLATE = """\
-- Generated via ./bqetl generate stable_views
CREATE OR REPLACE VIEW
  `{full_view_id}`
AS
SELECT
  * REPLACE(
    {replacements})
FROM
  `{target}`
"""

VIEW_METADATA_TEMPLATE = """\
# Generated via ./bqetl generate stable_views
---
friendly_name: Historical Pings for `{document_namespace}/{document_type}`
description: |-
  A historical view of pings sent for the
  `{document_namespace}/{document_type}`
  document type.

  This view is guaranteed to contain only complete days
  (per `submission_timestamp`)
  and to contain only one row per distinct `document_id` within a given date.

  Clustering fields: `normalized_channel`, `sample_id`
"""


def write_dataset_metadata_if_not_exists(
    target_project: str, sql_dir: Path, schema: SchemaFile
):
    """Write default dataset_metadata.yaml files where none exist.

    This function expects to be handed one representative `SchemaFile`
    object representing the dataset.
    """
    from bigquery_etl.metadata.parse_metadata import DatasetMetadata

    dataset_family = schema.bq_dataset_family
    project_dir = sql_dir / target_project

    # Derived dataset
    dataset_name = f"{dataset_family}_derived"
    target = project_dir / dataset_name / "dataset_metadata.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        print(f"Creating {target}")
        DatasetMetadata(
            friendly_name=f"{schema.document_namespace} Derived",
            description=(
                f"Derived tables related to document namespace"
                f" {schema.document_namespace},"
                f" usually populated via queries defined in"
                f" https://github.com/mozilla/bigquery-etl"
                f" and managed by Airflow"
            ),
            dataset_base_acl="derived",
            user_facing=False,
        ).write(target)

    # User-facing dataset
    dataset_name = dataset_family
    target = project_dir / dataset_name / "dataset_metadata.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        print(f"Creating {target}")
        DatasetMetadata(
            friendly_name=f"{schema.document_namespace}",
            description=(
                f"User-facing views related to document namespace"
                f" {schema.document_namespace}; see https://github.com/"
                f"mozilla-services/mozilla-pipeline-schemas/tree/"
                f"generated-schemas/schemas/{schema.document_namespace}"
            ),
            dataset_base_acl="view",
            user_facing=True,
        ).write(target)


def write_view_if_not_exists(target_project: str, sql_dir: Path, schema: SchemaFile):
    """If a view.sql does not already exist, write one to the target directory."""
    # add imports here to run in multiple processes via pathos
    from bigquery_etl.format_sql.formatter import reformat
    from bigquery_etl.schema import Schema
    from sql_generators.stable_views import VIEW_METADATA_TEMPLATE, VIEW_QUERY_TEMPLATE
    import re

    VIEW_CREATE_REGEX = re.compile(
        r"CREATE OR REPLACE VIEW\n\s*[^\s]+\s*\nAS", re.IGNORECASE
    )

    target_dir = (
        sql_dir
        / target_project
        / schema.bq_dataset_family
        / schema.bq_table_unversioned
    )

    target_file = target_dir / "view.sql"

    if target_file.exists():
        return

    full_source_id = f"{target_project}.{schema.stable_table}"
    full_view_id = f"{target_project}.{schema.user_facing_view}"
    replacements = ["mozfun.norm.metadata(metadata) AS metadata"]
    if schema.schema_id == "moz://mozilla.org/schemas/glean/ping/1":
        replacements += ["mozfun.norm.glean_ping_info(ping_info) AS ping_info"]
        if schema.bq_table == "baseline_v1":
            replacements += [
                "mozfun.norm.glean_baseline_client_info"
                "(client_info, metrics) AS client_info"
            ]
        if (
            schema.bq_dataset_family == "org_mozilla_fenix"
            and schema.bq_table == "metrics_v1"
        ):
            # todo: use mozfun udfs
            replacements += [
                "mozdata.udf.normalize_fenix_metrics"
                "(client_info.telemetry_sdk_build, metrics)"
                " AS metrics"
            ]
        if schema.bq_dataset_family == "firefox_desktop":
            # FOG does not provide an app_name, so we inject the one that
            # people already associate with desktop Firefox per bug 1672191.
            replacements += [
                "'Firefox' AS normalized_app_name",
            ]
    elif schema.schema_id.startswith("moz://mozilla.org/schemas/main/ping/"):
        replacements += ["mozdata.udf.normalize_main_payload(payload) AS payload"]
    replacements_str = ",\n    ".join(replacements)
    full_sql = reformat(
        VIEW_QUERY_TEMPLATE.format(
            target=full_source_id,
            replacements=replacements_str,
            full_view_id=full_view_id,
        ),
        trailing_newline=True
    )
    print(f"Creating {target_file}")
    target_dir.mkdir(parents=True, exist_ok=True)
    with target_file.open("w") as f:
        f.write(full_sql)
    metadata_content = VIEW_METADATA_TEMPLATE.format(
        document_namespace=schema.document_namespace,
        document_type=schema.document_type,
    )
    metadata_file = target_dir / "metadata.yaml"
    if not metadata_file.exists():
        with metadata_file.open("w") as f:
            f.write(metadata_content)

    # get view schema with descriptions
    try:
        content = VIEW_CREATE_REGEX.sub("", target_file.read_text())
        content += " WHERE DATE(submission_timestamp) = '2020-01-01'"
        view_schema = Schema.from_query_file(target_file, content=content)

        stable_table_schema = Schema.from_json({"fields": schema.schema})
        view_schema.merge(stable_table_schema, add_missing_fields=False)
        view_schema.to_yaml_file(target_dir / "schema.yaml")
    except Exception as e:
        print(f"Cannot generate schema.yaml for {target_file}: {e}")


@click.command("generate")
@click.option(
    "--target_project",
    "--target-project",
    help="Which project the views should be written to.",
    default="moz-fx-data-shared-prod",
)
@click.option(
    "--output-dir",
    "--output_dir",
    "--sql-dir",
    "--sql_dir",
    help="The location to write to. Defaults to sql/.",
    default=Path("sql"),
    type=click.Path(file_okay=False),
)
@click.option(
    "--log-level",
    "--log_level",
    help="Log level.",
    default=logging.getLevelName(logging.INFO),
    type=str.upper,
)
@click.option(
    "--parallelism",
    "-P",
    help="Maximum number of tasks to execute concurrently",
    default=20,
    type=int,
)
def generate(target_project, output_dir, log_level, parallelism):
    """
    Generate view definitions.

    Metadata about document types is from the generated-schemas branch
    of mozilla-pipeline-schemas. We write out generated views for each
    document type in parallel.

    There is a performance bottleneck here due to the need to dry-run each
    view to ensure the source table actually exists.
    """
    # set log level
    logging.basicConfig(level=log_level, format="%(levelname)s %(message)s")

    schemas = get_stable_table_schemas()
    one_schema_per_dataset = [
        last for k, (*_, last) in groupby(schemas, lambda t: t.bq_dataset_family)
    ]

    with ProcessingPool(parallelism) as pool:
        pool.map(
            partial(
                write_view_if_not_exists,
                target_project,
                Path(output_dir),
            ),
            schemas,
        )
        pool.map(
            partial(
                write_dataset_metadata_if_not_exists,
                target_project,
                Path(output_dir),
            ),
            one_schema_per_dataset,
        )
