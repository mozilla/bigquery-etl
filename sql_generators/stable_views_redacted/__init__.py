"""
Generate redacted views on top of stable views.

Creates views that exclude sensitive metrics of highly_sensitive categories.

If a view.sql already exists at the target path, no file will be written,
allowing manual overrides by checking files into the sql/ tree.
"""

import logging
import re
from collections import defaultdict
from pathlib import Path

import click
import requests

from bigquery_etl.cli.utils import use_cloud_function_option
from bigquery_etl.config import ConfigLoader
from bigquery_etl.dryrun import get_id_token
from bigquery_etl.format_sql.formatter import reformat
from bigquery_etl.schema import Schema
from bigquery_etl.schema.stable_table_schema import get_stable_table_schemas

PROBEINFO_URL = "https://probeinfo.telemetry.mozilla.org"
APP_LISTINGS_URL = f"{PROBEINFO_URL}/v2/glean/app-listings"

REDACTED_VIEW_QUERY_TEMPLATE = """\
-- Generated via ./bqetl generate stable_views_redacted
CREATE OR REPLACE VIEW
  `{full_view_id}`
AS
SELECT
  * REPLACE(
    {metrics_replacement}
  )
FROM
  `{source_view}`
"""

REDACTED_METADATA_TEMPLATE = """\
# Generated via ./bqetl generate stable_views_redacted
---
friendly_name: Redacted Pings for `{document_namespace}/{document_type}`
description: |-
  A redacted view of `{source_view_short}` with sensitive metrics excluded.
  Fields with data_sensitivity categories [{SENSITIVITY_CATEGORIES}] are removed.

  See the full (access-restricted) data in `{source_view_short}`.
"""

# Probeinfo metric types that get a "2" suffix in BQ due to ingestion pipeline fix.
# For glean/ping/1 views, stable_views aliases these back (text2 -> text).
# For glean-min/ping/1 views, the suffix remains.
# Discrepancy between glean and glean-min is a bug in view generation logic
# See https://bugzilla.mozilla.org/show_bug.cgi?id=1741487
SUFFIXED_METRIC_TYPES = {
    "text": "text2",
    "url": "url2",
    "jwe": "jwe2",
    "labeled_rate": "labeled_rate2",
}

# Metrics with these data_sensitivity categories are excluded from redacted views.
SENSITIVITY_CATEGORIES = ["highly_sensitive"]


def _get_glean_app_metrics(v1_name):
    """Return a dictionary of the Glean app's metrics."""
    resp = requests.get(f"{PROBEINFO_URL}/glean/{v1_name}/metrics")
    resp.raise_for_status()
    return resp.json()


def _get_sensitive_metrics(all_metrics, ping_name):
    """Filter probeinfo metrics to sensitive ones for a given ping.

    Returns list of dicts: {metric_key, bq_column_name, metric_type}.
    """
    sensitive = []
    for key, val in all_metrics.items():
        history = val.get("history", [])
        if not history:
            continue
        latest = history[-1]
        pings = latest.get("send_in_pings", [])
        if ping_name not in pings:
            continue
        data_sensitivity = latest.get("data_sensitivity", [])
        if not any(cat in data_sensitivity for cat in SENSITIVITY_CATEGORIES):
            continue
        metric_type = val.get("type", latest.get("type", ""))
        bq_column_name = key.replace(".", "_")
        sensitive.append(
            {
                "metric_key": key,
                "bq_column_name": bq_column_name,
                "metric_type": metric_type,
            }
        )
    return sensitive


def _get_metrics_struct_fields(schema_fields):
    """Extract the metrics RECORD's sub-struct names and their column names from BQ schema.

    Note: This reads from the raw stable TABLE schema (from mozilla-pipeline-schemas).
    For glean-min/ping/1 schemas, field names match the stable view output directly.
    For glean/ping/1 schemas, the stable view aliases text2->text etc. (see SUFFIXED_METRIC_TYPES),
    so the raw schema names may differ from the stable view output. Currently only
    glean-min/ping/1 pings are configured, so this distinction doesn't apply.

    Returns dict: {"text2": ["col1", ...], "quantity": ["col1", ...], ...}
    """
    metrics_field = next((f for f in schema_fields if f["name"] == "metrics"), None)
    if metrics_field is None:
        return {}

    result = {}
    for sub_struct in metrics_field.get("fields", []):
        field_names = [f["name"] for f in sub_struct.get("fields", [])]
        result[sub_struct["name"]] = field_names
    return result


def _resolve_bq_type_name(probeinfo_type, schema_id, metrics_struct_types):
    """Map probeinfo metric type to actual BQ field name in the stable view output.

    For glean/ping/1 views (which alias text2->text): "text" -> "text"
    For glean-min/ping/1 views (no aliasing): "text" -> "text2"
    Falls back to checking what actually exists in the schema.

    TODO: Once the glean/glean-min stable view discrepancy is fixed (so both alias
    text2->text consistently), this function and SUFFIXED_METRIC_TYPES can be removed.
    The probeinfo metric type name will match the stable view field name directly.
    """
    is_glean_ping_1 = schema_id == "moz://mozilla.org/schemas/glean/ping/1"

    if probeinfo_type in SUFFIXED_METRIC_TYPES:
        suffixed = SUFFIXED_METRIC_TYPES[probeinfo_type]
        if is_glean_ping_1:
            # glean/ping/1 views alias text2->text, so the view exposes "text"
            if probeinfo_type in metrics_struct_types:
                return probeinfo_type
            # fallback: maybe only the suffixed version exists
            if suffixed in metrics_struct_types:
                return suffixed
        else:
            # glean-min/ping/1 views keep the suffix
            if suffixed in metrics_struct_types:
                return suffixed
            if probeinfo_type in metrics_struct_types:
                return probeinfo_type

    # Direct match for non-suffixed types
    if probeinfo_type in metrics_struct_types:
        return probeinfo_type

    return None


def _build_metrics_replacement(sensitive_metrics, metrics_struct_types, schema_id):
    """Build the SQL REPLACE clause for the metrics struct.

    Groups sensitive metrics by their BQ type name, then:
    - If ALL fields in a type are sensitive -> EXCEPT the type entirely
    - If SOME fields are sensitive -> REPLACE with EXCEPT on sub-struct
    """
    # Group by resolved BQ type
    by_type = defaultdict(list)
    for m in sensitive_metrics:
        bq_type = _resolve_bq_type_name(
            m["metric_type"], schema_id, metrics_struct_types
        )
        if bq_type is None:
            raise ValueError(
                f"Could not resolve BQ type for sensitive metric {m['metric_key']} "
                f"(probeinfo type: {m['metric_type']}). "
                f"Refusing to generate a redacted view that would leak sensitive data."
            )
        by_type[bq_type].append(m["bq_column_name"])

    if not by_type:
        return None

    except_types = []  # types to fully exclude
    replace_clauses = []  # types to partially exclude

    for bq_type, columns in sorted(by_type.items()):
        all_columns_in_type = metrics_struct_types.get(bq_type, [])
        if set(columns) >= set(all_columns_in_type):
            # All fields are sensitive, exclude the entire type
            except_types.append(bq_type)
        else:
            # Only some fields are sensitive
            except_list = ", ".join(sorted(columns))
            replace_clauses.append(
                f"(SELECT AS STRUCT metrics.{bq_type}.* EXCEPT({except_list})) AS {bq_type}"
            )

    # Build the combined SQL
    parts = []
    if except_types:
        parts.append("metrics.* EXCEPT(" + ", ".join(sorted(except_types)) + ")")
    else:
        parts.append("metrics.*")

    if replace_clauses:
        if except_types:
            # When we have both EXCEPT and REPLACE, wrap in SELECT AS STRUCT with REPLACE
            inner = (
                "metrics.* EXCEPT("
                + ", ".join(sorted(except_types))
                + ") REPLACE("
                + ", ".join(replace_clauses)
                + ")"
            )
        else:
            inner = "metrics.* REPLACE(" + ", ".join(replace_clauses) + ")"
        return "(SELECT AS STRUCT " + inner + ") AS metrics"
    elif except_types:
        return "(SELECT AS STRUCT " + parts[0] + ") AS metrics"
    else:
        return None


def _write_redacted_view(
    target_project,
    sql_dir,
    schema_file,
    sensitive_metrics,
    metrics_struct_types,
    id_token=None,
):
    """Write view.sql, metadata.yaml, and schema.yaml for a redacted view."""
    VIEW_CREATE_REGEX = re.compile(
        r"CREATE OR REPLACE VIEW\n\s*[^\s]+\s*\nAS", re.IGNORECASE
    )

    ping_name = schema_file.bq_table_unversioned
    target_dir = (
        sql_dir
        / target_project
        / schema_file.bq_dataset_family
        / f"{ping_name}_redacted"
    )
    target_file = target_dir / "view.sql"

    if target_file.exists():
        logging.info(f"Skipping {target_file} (already exists)")
        return

    metrics_replacement = _build_metrics_replacement(
        sensitive_metrics, metrics_struct_types, schema_file.schema_id
    )
    if metrics_replacement is None:
        logging.warning(
            f"No metrics to redact for {schema_file.bq_dataset_family}.{ping_name}, skipping"
        )
        return

    full_view_id = (
        f"{target_project}.{schema_file.bq_dataset_family}.{ping_name}_redacted"
    )
    source_view = f"{target_project}.{schema_file.user_facing_view}"

    full_sql = reformat(
        REDACTED_VIEW_QUERY_TEMPLATE.format(
            full_view_id=full_view_id,
            source_view=source_view,
            metrics_replacement=metrics_replacement,
        ),
        trailing_newline=True,
    )

    logging.info(f"Creating {target_file}")
    target_dir.mkdir(parents=True, exist_ok=True)
    with target_file.open("w") as f:
        f.write(full_sql)

    # Write metadata
    sensitivity_str = ", ".join(sorted(SENSITIVITY_CATEGORIES))
    metadata_content = REDACTED_METADATA_TEMPLATE.format(
        document_namespace=schema_file.document_namespace,
        document_type=schema_file.document_type,
        source_view_short=schema_file.user_facing_view,
        SENSITIVITY_CATEGORIES=sensitivity_str,
    )
    metadata_file = target_dir / "metadata.yaml"
    if not metadata_file.exists():
        with metadata_file.open("w") as f:
            f.write(metadata_content)

    # Write schema.yaml via dry-run
    try:
        content = VIEW_CREATE_REGEX.sub("", target_file.read_text())
        content += " WHERE DATE(submission_timestamp) = '2020-01-01'"
        view_schema = Schema.from_query_file(
            target_file, content=content, sql_dir=sql_dir, id_token=id_token
        )
        view_schema.to_yaml_file(target_dir / "schema.yaml")
    except Exception as e:
        logging.error(f"Cannot generate schema.yaml for {target_file}: {e}")


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
@use_cloud_function_option
def generate(target_project, output_dir, log_level, use_cloud_function):
    """Generate redacted view definitions.

    Creates views that exclude metrics with configured data_sensitivity
    categories from stable views. Uses probeinfo API to identify sensitive
    metrics and generates EXCEPT/REPLACE patterns.
    """
    logging.basicConfig(level=log_level, format="%(levelname)s %(message)s")
    sql_dir = Path(output_dir)

    pings_config = ConfigLoader.get(
        "generate", "stable_views_redacted", "pings", fallback={}
    )

    if not pings_config:
        logging.info("No pings configured for stable_views_redacted, nothing to do")
        return

    # Get all stable table schemas (cached)
    schemas = get_stable_table_schemas()

    # Get app listings for v1_name lookup
    resp = requests.get(APP_LISTINGS_URL)
    resp.raise_for_status()
    app_listings = resp.json()
    family_to_v1 = {}
    for app in app_listings:
        fam = app.get("bq_dataset_family")
        if fam and fam not in family_to_v1:
            family_to_v1[fam] = app["v1_name"]

    id_token = get_id_token()

    for bq_dataset_family, ping_names in pings_config.items():
        v1_name = family_to_v1.get(bq_dataset_family)
        if v1_name is None:
            logging.warning(f"Could not find v1_name for {bq_dataset_family}, skipping")
            continue

        # Fetch all metrics for this app
        all_metrics = _get_glean_app_metrics(v1_name)

        for ping_name in ping_names:
            # Find the matching schema
            schema_file = next(
                (
                    s
                    for s in schemas
                    if s.bq_dataset_family == bq_dataset_family
                    and s.bq_table_unversioned == ping_name
                ),
                None,
            )
            if schema_file is None:
                logging.warning(
                    f"No schema found for {bq_dataset_family}.{ping_name}, skipping"
                )
                continue

            # Get sensitive metrics for this ping
            sensitive = _get_sensitive_metrics(all_metrics, ping_name)
            if not sensitive:
                logging.info(
                    f"No sensitive metrics found for {bq_dataset_family}.{ping_name}, skipping"
                )
                continue

            logging.info(
                f"Found {len(sensitive)} sensitive metrics for "
                f"{bq_dataset_family}.{ping_name}: "
                f"{[m['metric_key'] for m in sensitive]}"
            )

            # Get the metrics struct field info from the schema
            metrics_struct_types = _get_metrics_struct_fields(schema_file.schema)

            _write_redacted_view(
                target_project=target_project,
                sql_dir=sql_dir,
                schema_file=schema_file,
                sensitive_metrics=sensitive,
                metrics_struct_types=metrics_struct_types,
                id_token=id_token,
            )


if __name__ == "__main__":
    generate()
