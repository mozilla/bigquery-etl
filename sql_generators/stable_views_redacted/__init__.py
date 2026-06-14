"""
Generate redacted views on top of stable tables.

Extends the stable_views generator by reusing its view transformations
(metadata normalization, bot detection, etc.) and adding exclusions for
metrics with sensitive data_sensitivity categories.

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
from sql_generators.glean_usage.common import APP_LISTINGS_URL, get_glean_app_metrics

REDACTED_METADATA_TEMPLATE = """\
# Generated via ./bqetl generate stable_views_redacted
---
friendly_name: Redacted Pings for `{document_namespace}/{document_type}`
description: |-
  A redacted view of `{source_table_short}` with sensitive metrics excluded.
  Fields with data_sensitivity categories [{SENSITIVITY_CATEGORIES}] are removed.

  See the full (access-restricted) data in `{source_table_short}` and `{source_view_short}`.
labels:
    authorized: true
"""

# Metrics with these data_sensitivity categories are excluded from redacted views.
SENSITIVITY_CATEGORIES = ["highly_sensitive"]


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


def _resolve_bq_type_name(probeinfo_type, metrics_struct_types):
    """Map probeinfo metric type to actual BQ field name in the raw stable table schema.

    Probeinfo uses canonical type names (e.g. "text") but the raw stable table may
    store these under a "2" suffix (e.g. "text2") due to a historical ingestion fix.
    This function checks what actually exists in the schema.
    """
    # Direct match
    if probeinfo_type in metrics_struct_types:
        return probeinfo_type

    # Try with "2" suffix (historical ingestion fix: text->text2, url->url2, etc.)
    suffixed = probeinfo_type + "2"
    if suffixed in metrics_struct_types:
        return suffixed

    return None


def _add_metrics_redaction(replacements, sensitive_metrics, metrics_struct_types):
    """Add sensitive metric exclusions to the replacements list.

    Groups sensitive metrics by BQ type, then for each type either excludes it
    entirely (all fields sensitive) or partially (EXCEPT on sub-struct fields).
    """
    # Group sensitive metrics by resolved BQ type name
    by_type = defaultdict(list)
    for m in sensitive_metrics:
        bq_type = _resolve_bq_type_name(m["metric_type"], metrics_struct_types)
        if bq_type is None:
            raise ValueError(
                f"Could not resolve BQ type for sensitive metric {m['metric_key']} "
                f"(probeinfo type: {m['metric_type']}). "
                f"Refusing to generate a redacted view that would leak sensitive data."
            )
        by_type[bq_type].append(m["bq_column_name"])

    if not by_type:
        return None

    except_types = []
    replace_clauses = []

    for bq_type, columns in sorted(by_type.items()):
        all_columns_in_type = metrics_struct_types.get(bq_type, [])
        if set(columns) >= set(all_columns_in_type):
            except_types.append(bq_type)
        else:
            except_list = ", ".join(sorted(columns))
            replace_clauses.append(
                f"(SELECT AS STRUCT metrics.{bq_type}.* EXCEPT({except_list})) AS {bq_type}"
            )

    # Build the metrics replacement SQL
    if replace_clauses:
        if except_types:
            inner = (
                "metrics.* EXCEPT("
                + ", ".join(sorted(except_types))
                + ") REPLACE("
                + ", ".join(replace_clauses)
                + ")"
            )
        else:
            inner = "metrics.* REPLACE(" + ", ".join(replace_clauses) + ")"
    else:
        inner = "metrics.* EXCEPT(" + ", ".join(sorted(except_types)) + ")"

    return replacements + ["(SELECT AS STRUCT " + inner + ") AS metrics"]


def _write_redacted_view(
    target_project,
    sql_dir,
    schema_file,
    sensitive_metrics,
    metrics_struct_types,
    id_token=None,
):
    """Write view.sql, metadata.yaml, and schema.yaml for a redacted view."""
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

    # Import here to avoid circular import at module load time
    from sql_generators.stable_views import (
        BOT_GENERATED,
        VIEW_QUERY_TEMPLATE_NO_CLIENT_INFO,
    )

    VIEW_CREATE_REGEX = re.compile(
        r"CREATE OR REPLACE VIEW\n\s*[^\s]+\s*\nAS", re.IGNORECASE
    )

    # Only glean-min/ping schemas are currently supported. For glean/ping,
    # the stable_views generator applies complex metrics transformations
    # (text2->text aliasing, datetime parsing, etc.) that we can't easily merge
    # with the redaction logic. Use a manual view.sql override for those.
    if schema_file.schema_id in (
        "moz://mozilla.org/schemas/glean/ping/1",
        "moz://mozilla.org/schemas/glean/ping/2",
    ):
        raise NotImplementedError(
            f"Redacted view generation for glean/ping/1 and glean/ping/2 schemas "
            f"is not yet supported "
            f"({schema_file.bq_dataset_family}.{ping_name}). "
            f"Use a manual view.sql override instead."
        )

    # Base replacements matching what stable_views generates for glean-min pings
    replacements = ["mozfun.norm.metadata(metadata) AS metadata"]

    # Add sensitive metric exclusions
    replacements = _add_metrics_redaction(
        replacements, sensitive_metrics, metrics_struct_types
    )
    if replacements is None:
        logging.warning(
            f"No metrics to redact for {schema_file.bq_dataset_family}.{ping_name}, skipping"
        )
        return

    full_view_id = (
        f"{target_project}.{schema_file.bq_dataset_family}.{ping_name}_redacted"
    )
    full_source_id = f"{target_project}.{schema_file.stable_table}"
    replacements_str = ",\n    ".join(replacements)

    full_sql = reformat(
        VIEW_QUERY_TEMPLATE_NO_CLIENT_INFO.format(
            target=full_source_id,
            replacements=replacements_str,
            full_view_id=full_view_id,
            bot_generated=BOT_GENERATED,
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
        source_table_short=schema_file.stable_table,
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
        all_metrics = get_glean_app_metrics(v1_name)

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
