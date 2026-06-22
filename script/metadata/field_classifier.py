"""Classify each profiled column against the Mozilla data taxonomy.

Reads:
  - akomar_column_profiles_v1             (Phase 1: per-column profiling stats)
  - akomar_metadata_phase2_table_pings_v1 (Phase 2: source ping per table)
  - akomar_metadata_phase2_ping_probes_v1 (Phase 2: probes w/ data_sensitivity, tags)
  - source table COLUMN_FIELD_PATHS       (existing BigQuery column descriptions)
  - classification/taxonomy.json          (preprocessed taxonomy)

Writes:
  - akomar_field_classifications_v1 - one row per column per table

Working-table project/dataset are configurable via CLASSIFICATION_PROJECT /
CLASSIFICATION_DATASET (default mozdata-nonprod.analysis).
"""

import json
import logging
import os
import re
from argparse import ArgumentParser
from datetime import datetime, timezone
from functools import partial
from pathlib import Path

import anthropic
from google import genai
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.genai.types import HttpOptions

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)

# All working tables live in one (project, dataset). Override via env to target a
# dev sandbox, e.g. CLASSIFICATION_PROJECT=moz-fx-data-proto
# CLASSIFICATION_DATASET=<your-dataset>. Defaults to mozdata-nonprod.analysis.
ANALYSIS_PROJECT = os.environ.get("CLASSIFICATION_PROJECT", "mozdata-nonprod")
ANALYSIS_DATASET = os.environ.get("CLASSIFICATION_DATASET", "analysis")
_ANALYSIS = f"{ANALYSIS_PROJECT}.{ANALYSIS_DATASET}"
PHASE1_TABLE = f"{_ANALYSIS}.akomar_column_profiles_v1"
MAPPING_TABLE = f"{_ANALYSIS}.akomar_metadata_phase2_table_pings_v1"
PROBE_TABLE = f"{_ANALYSIS}.akomar_metadata_phase2_ping_probes_v1"
DEST_TABLE = f"{_ANALYSIS}.akomar_field_classifications_v1"
DEST_PROJECT = ANALYSIS_PROJECT
DEFAULT_MODEL = "claude-sonnet-4-6"
GEMINI_VERTEX_PROJECT = "mozdata"
GEMINI_VERTEX_LOCATION = "global"
TAXONOMY_PATH = Path(__file__).parent / "classification" / "taxonomy.json"
TOP_N_PROBES = 3


def is_claude_model(name):
    """Anthropic-hosted Claude model name (e.g. claude-sonnet-4-6)."""
    return name.startswith("claude-")


def is_gemini_model(name):
    """Vertex-hosted Gemini model name (e.g. gemini-3.1-flash-lite-preview)."""
    return name.startswith("gemini-")


DEST_SCHEMA = [
    bigquery.SchemaField("source_project", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_dataset", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_table", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("data_type", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "primary_label",
        "STRING",
        mode="NULLABLE",
        description="Most specific matching taxonomy label (e.g. user.unique_id.client_id).",
    ),
    bigquery.SchemaField(
        "secondary_labels",
        "STRING",
        mode="REPEATED",
        description="Additional taxonomy labels that also apply.",
    ),
    bigquery.SchemaField(
        "confidence",
        "STRING",
        mode="NULLABLE",
        description="high | medium | low - the model's self-reported confidence.",
    ),
    bigquery.SchemaField(
        "reasoning",
        "STRING",
        mode="NULLABLE",
        description="1-2 sentence justification referencing the signals used.",
    ),
    bigquery.SchemaField(
        "needs_review",
        "BOOLEAN",
        mode="NULLABLE",
        description="True when confidence is low or signals conflict.",
    ),
    bigquery.SchemaField("matched_probe", "STRING", mode="NULLABLE"),
    bigquery.SchemaField(
        "data_sensitivity",
        "STRING",
        mode="REPEATED",
        description="Glean data_sensitivity labels from the matched probe, if any.",
    ),
    bigquery.SchemaField(
        "model",
        "STRING",
        mode="NULLABLE",
        description="Full LLM model name that produced the row, e.g. claude-sonnet-4-6 or gemini-3.1-flash-lite-preview.",
    ),
    bigquery.SchemaField("classified_at", "TIMESTAMP", mode="REQUIRED"),
]


# --- Taxonomy ---


def load_taxonomy():
    """Load the preprocessed taxonomy JSON."""
    return json.loads(TAXONOMY_PATH.read_text())


def taxonomy_prompt_block(taxonomy):
    """Compact the taxonomy into a single JSON block for the prompt."""
    compact = [
        {
            "label": e["label"],
            "name": e.get("display_name") or "",
            "desc": e.get("description") or "",
            "examples": e.get("examples") or "",
        }
        for e in taxonomy
    ]
    return json.dumps(compact, separators=(",", ":"))


# --- Probe matching (fuzzy column-to-probe name match, top-3 only) ---


def normalize_name(name):
    """Strip all non-alphanumeric characters and lowercase for fuzzy matching."""
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def find_matching_probes(column_name, probes):
    """Return up to TOP_N_PROBES candidate probes for a column name."""
    col_norm = normalize_name(column_name)
    scored = []
    for probe in probes:
        pname = probe.get("probe_name") or ""
        if not pname:
            continue
        pname_norm = normalize_name(pname)
        if col_norm == pname_norm:
            score = 3
        elif pname_norm.endswith(col_norm) or pname_norm.startswith(col_norm):
            score = 2
        elif col_norm in pname_norm or pname_norm in col_norm:
            score = 1
        else:
            continue
        scored.append((score, probe))
    scored.sort(key=lambda x: -x[0])
    return [p for _, p in scored[:TOP_N_PROBES]]


# --- Prompt ---


def _profile_block(profile):
    """Render the observed profiling signals for one column as prompt text."""
    lines = []
    if profile.get("is_glean_metric"):
        return (
            "Glean metric column - not data-profiled; classify from the metric"
            " definition and its data_sensitivity."
        )
    if profile.get("column_tier") == "pii_suppressed":
        lines.append(
            "PII-suppressed: column name matched a known PII pattern, so it was"
            " not scanned. Treat as personal data."
        )
    null_rate = profile.get("null_rate")
    if null_rate is not None:
        lines.append(f"Null rate: {null_rate}%")
    distinct_count = profile.get("distinct_count")
    if distinct_count is not None:
        hc = " (high cardinality)" if profile.get("is_high_cardinality") else ""
        lines.append(f"Distinct values: {distinct_count}{hc}")
    values = profile.get("values") or []
    if values:
        shown = ", ".join(f"{str(v)!r} ({f:,})" for v, f in values[:10])
        lines.append(f"Top values (value: frequency): {shown}")
    elif profile.get("example_value") is not None:
        lines.append(f"Example value: {profile['example_value']}")
    description = profile.get("pass1_description")
    if description:
        lines.append(f"Description (from profiling): {description}")
    return "\n".join(lines) if lines else "No profiling stats available."


def build_classification_prompt(
    column_name, data_type, table, profile, matching_probes, taxonomy_json
):
    """Build a prompt asking the LLM to assign a taxonomy label."""
    if matching_probes:
        probe_lines = []
        for p in matching_probes:
            parts = [f"name={p['probe_name']}"]
            if p.get("probe_type"):
                parts.append(f"type={p['probe_type']}")
            if p.get("data_sensitivity"):
                parts.append(f"data_sensitivity={p['data_sensitivity']}")
            if p.get("tags"):
                parts.append(f"tags={p['tags']}")
            if p.get("probe_description"):
                parts.append(f"desc={p['probe_description']}")
            probe_lines.append("  - " + " | ".join(parts))
        probes_section = "Candidate probes:\n" + "\n".join(probe_lines)
    else:
        probes_section = "Candidate probes: none matched."

    existing_desc = profile.get("bq_description")
    desc_line = (
        f"Existing column description (from BigQuery schema): {existing_desc}\n"
        if existing_desc
        else ""
    )

    return (
        "You are classifying a BigQuery column against Mozilla's data taxonomy.\n\n"
        f"Table: {table}\n"
        f"Column: {column_name}\n"
        f"Data type: {data_type}\n"
        f"{desc_line}"
        f"Profiled data:\n{_profile_block(profile)}\n\n"
        f"{probes_section}\n\n"
        "Taxonomy (JSON list of {label, name, desc, examples}):\n"
        f"{taxonomy_json}\n\n"
        "Pick the single most specific taxonomy label that fits. If multiple apply,"
        " list the extras in secondary_labels. Use the Glean data_sensitivity signal"
        " to disambiguate when present (e.g. highly_sensitive strongly implies"
        " user.behavior, user.content, user.location.precise, etc.).\n\n"
        "Respond with a JSON object only (no markdown fences):\n"
        '{"primary_label": "<label>", "secondary_labels": [], '
        '"confidence": "high|medium|low", "reasoning": "<1-2 sentences>", '
        '"needs_review": true|false}'
    )


def _strip_json_fences(text):
    """Remove optional ```json ... ``` markdown fences."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text


def call_claude(claude_client, model, prompt):
    """Call Claude and parse the JSON response."""
    response = claude_client.messages.create(
        model=model,
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    return json.loads(_strip_json_fences(response.content[0].text))


def call_gemini(gemini_client, model, prompt):
    """Call Gemini via Vertex and parse the JSON response."""
    response = gemini_client.models.generate_content(
        model=model,
        contents=prompt,
    )
    return json.loads(_strip_json_fences(response.text))


# --- BQ loaders ---


def _profiling_columns(bq_client):
    """Return the set of column names present in the profiling table."""
    proj, ds, tbl = PHASE1_TABLE.split(".")
    query = f"""
        SELECT column_name
        FROM `{proj}.{ds}`.INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = '{tbl}'
    """
    return {r.column_name for r in bq_client.query(query).result()}


def load_phase1(bq_client, project=None, dataset=None, table=None):
    """Load profiled columns grouped by (project, dataset, table).

    Reads the productionized column-profiles schema. Excludes only
    'undocumented' columns (no profiling signal); keeps pii_suppressed,
    scalar_array, and nested_leaf. Picks the latest profiled_at snapshot per
    column (the table accumulates weekly snapshots). pass1_description is
    optional - selected only when the profiling table has that column,
    otherwise NULL.
    """
    has_description = "pass1_description" in _profiling_columns(bq_client)
    desc_select = (
        "pass1_description"
        if has_description
        else "CAST(NULL AS STRING) AS pass1_description"
    )

    where = "WHERE column_tier != 'undocumented'"
    if project and dataset and table:
        where += (
            f" AND source_project = '{project}'"
            f" AND source_dataset = '{dataset}'"
            f" AND source_table = '{table}'"
        )
    query = f"""
        SELECT source_project, source_dataset, source_table,
               column_name, data_type, column_tier,
               null_rate, distinct_count, is_high_cardinality,
               example_value, `values`,
               {desc_select}
        FROM `{PHASE1_TABLE}`
        {where}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY source_project, source_dataset, source_table, column_name
            ORDER BY profiled_at DESC
        ) = 1
        ORDER BY source_dataset, source_table, column_name
    """
    rows_by_table = {}
    for row in bq_client.query(query).result():
        key = (row.source_project, row.source_dataset, row.source_table)
        # row["values"] (not row.values - Row.values is a method) is a repeated
        # RECORD of {value, frequency}; flatten to (value, frequency) tuples.
        values = [(v["value"], v["frequency"]) for v in (row["values"] or [])]
        rows_by_table.setdefault(key, []).append(
            {
                "column_name": row.column_name,
                "data_type": row.data_type,
                "column_tier": row.column_tier,
                "null_rate": row.null_rate,
                "distinct_count": row.distinct_count,
                "is_high_cardinality": row.is_high_cardinality,
                "example_value": row.example_value,
                "values": values,
                "pass1_description": row.pass1_description,
            }
        )
    return rows_by_table


def load_descriptions(bq_client, project, dataset, table):
    """Return {field_path: description} for columns with a non-empty BQ description.

    Read straight from the source table's COLUMN_FIELD_PATHS (the profiler does
    not capture descriptions), so existing curated descriptions can inform
    classification. field_path uses dot notation, matching column_name in the
    profiling rows.
    """
    query = f"""
        SELECT field_path, description
        FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        WHERE table_name = @table_name
          AND description IS NOT NULL AND description != ''
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", table)]
    )
    return {
        row.field_path: row.description
        for row in bq_client.query(query, job_config=job_config).result()
    }


def load_metric_leaf_columns(bq_client, project, dataset, table):
    """Return scalar leaf columns under the Glean ``metrics`` STRUCT.

    Glean metric values are sensitive, so the profiler never samples them and
    marks the whole ``metrics`` STRUCT as undocumented. To classify them we read
    the source table's COLUMN_FIELD_PATHS directly and keep only scalar leaves
    (data_type not a RECORD/STRUCT/ARRAY parent) at exactly
    ``metrics.<type>.<name>`` (3 path segments). The depth filter excludes the
    ``.key``/``.value`` sub-leaves of labeled-metric maps (e.g.
    metrics.labeled_counter.glean_error_*), which are Glean-internal structural
    fields, not product metrics. Parameterized on table name, mirroring
    load_descriptions.
    """
    query = f"""
        SELECT field_path, data_type
        FROM `{project}.{dataset}`.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
        WHERE table_name = @table_name
          AND STARTS_WITH(field_path, 'metrics.')
          AND ARRAY_LENGTH(SPLIT(field_path, '.')) = 3
          AND data_type NOT LIKE 'STRUCT%'
          AND data_type NOT LIKE 'ARRAY%'
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", table)]
    )
    return [
        {"column_name": row.field_path, "data_type": row.data_type}
        for row in bq_client.query(query, job_config=job_config).result()
    ]


def match_metric_columns(metric_columns, ping_probes, skip_columns=None):
    """Pair metric leaf columns with their product probe by exact name.

    A Glean scalar metric ``category.name`` materializes in BigQuery as
    ``metrics.<type>.<category>_<name>``, whose leaf segment normalizes to
    exactly the dotted probe name (normalize('account_user_id') ==
    normalize('account.user_id')). Matching on that exact normalized name -
    rather than the fuzzy substring match used for arbitrary profiled columns -
    maps each metric to its own probe (so account_user_id_sha256 pairs with
    account.user_id_sha256, not account.user_id) and prevents short generic
    leaves from matching unrelated probes. Columns in skip_columns (e.g. ones
    already profiled in phase 1) are dropped.

    Returns a list of (column dict, matching probes) tuples; only leaves with an
    exact probe match are kept.
    """
    skip = skip_columns or set()
    probes_by_norm = {}
    for probe in ping_probes:
        pname = probe.get("probe_name") or ""
        if pname:
            probes_by_norm.setdefault(normalize_name(pname), []).append(probe)
    matched = []
    for col in metric_columns:
        col_name = col["column_name"]
        if col_name in skip:
            continue
        leaf = col_name.rsplit(".", 1)[-1]
        probes = probes_by_norm.get(normalize_name(leaf))
        if not probes:
            continue
        matched.append((col, probes[:TOP_N_PROBES]))
    return matched


def load_ping_mapping(bq_client):
    """Load table → ping mapping.

    The phase-2 mapping table only exists once a Glean-sourced table has been
    run through lineage resolution; for non-Glean datasets it is absent, so a
    missing table degrades to "no mapping" rather than failing.
    """
    query = f"""
        SELECT source_project, source_dataset, source_table,
               source_ping, ping_platform
        FROM `{MAPPING_TABLE}`
    """
    mapping = {}
    try:
        rows = bq_client.query(query).result()
    except NotFound:
        return mapping
    for row in rows:
        mapping[(row.source_project, row.source_dataset, row.source_table)] = {
            "source_ping": row.source_ping,
            "ping_platform": row.ping_platform,
        }
    return mapping


def load_probes_by_ping(bq_client):
    """Load probe definitions grouped by (ping_platform, source_ping).

    Like the ping mapping, the probe table only exists once a Glean ping has
    been resolved; a missing table degrades to "no probes".
    """
    query = f"""
        SELECT ping_platform, source_ping, probe_name, probe_description,
               probe_type, data_sensitivity, tags
        FROM `{PROBE_TABLE}`
    """
    probes_by_ping = {}
    try:
        rows = bq_client.query(query).result()
    except NotFound:
        return probes_by_ping
    for row in rows:
        key = (row.ping_platform, row.source_ping)
        probes_by_ping.setdefault(key, []).append(
            {
                "probe_name": row.probe_name,
                "probe_description": row.probe_description,
                "probe_type": row.probe_type,
                "data_sensitivity": list(row.data_sensitivity or []),
                "tags": list(row.tags or []),
            }
        )
    return probes_by_ping


def get_already_classified(bq_client):
    """Return set of (project, dataset, table, column, model) already classified."""
    try:
        query = f"""
            SELECT source_project, source_dataset, source_table, column_name, model
            FROM `{DEST_TABLE}`
        """
        return {
            (r.source_project, r.source_dataset, r.source_table, r.column_name, r.model)
            for r in bq_client.query(query).result()
        }
    except NotFound:
        return set()


def save_to_bq(bq_client, records):
    """Append classification records to the destination table."""
    job_config = bigquery.LoadJobConfig(
        schema=DEST_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = bq_client.load_table_from_json(records, DEST_TABLE, job_config=job_config)
    job.result()
    logging.info(f"Saved {len(records)} rows to {DEST_TABLE}")


# --- Main ---


def parse_args():
    """Parse command line arguments."""
    parser = ArgumentParser(
        description="Classify profiled BQ columns against the Mozilla data taxonomy."
    )
    parser.add_argument(
        "--table",
        help="Fully qualified BQ table (project.dataset.table). If omitted, processes all Phase 1 tables.",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help=(
            "Full LLM model name. Names starting with 'claude-' route to the "
            "Anthropic API (requires ANTHROPIC_API_KEY); names starting with "
            "'gemini-' route to Vertex AI on project "
            f"'{GEMINI_VERTEX_PROJECT}' (requires application-default "
            f"credentials). Default: {DEFAULT_MODEL}."
        ),
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help=(
            "Delete existing classifications for the in-scope table(s) and this "
            "--model before classifying, so rows are recomputed. Without it, "
            "already-classified columns are skipped. Scoped to --model, so a "
            "multi-model run does not wipe a sibling model's rows."
        ),
    )
    args = parser.parse_args()
    if not (is_claude_model(args.model) or is_gemini_model(args.model)):
        parser.error(
            f"Unrecognized --model '{args.model}'. "
            "Expected a name starting with 'claude-' or 'gemini-'."
        )
    return args


def refresh_classifications(bq_client, project, dataset, table, model):
    """Delete existing classification rows for the scope + model so they redo.

    Scoped to the current model so a multi-model run does not wipe a sibling
    model's rows. With no table (whole-run scope), clears every row for the
    model. Ignores a missing destination table (nothing to clear yet).
    """
    where = [f"model = '{model}'"]
    if table:
        where.append(f"source_project = '{project}'")
        where.append(f"source_dataset = '{dataset}'")
        where.append(f"source_table = '{table}'")
    clause = " AND ".join(where)
    try:
        bq_client.query(f"DELETE FROM `{DEST_TABLE}` WHERE {clause}").result()
        logging.info(f"Refresh: cleared classifications WHERE {clause}")
    except NotFound:
        pass


def main():
    """Classify columns from Phase 1 against the Mozilla data taxonomy."""
    args = parse_args()
    bq_client = bigquery.Client(project=DEST_PROJECT)

    if is_claude_model(args.model):
        claude_client = anthropic.Anthropic()
        invoke_llm = partial(call_claude, claude_client, args.model)
        logging.info(f"Using Claude model: {args.model}")
    else:
        gemini_client = genai.Client(
            vertexai=True,
            project=GEMINI_VERTEX_PROJECT,
            location=GEMINI_VERTEX_LOCATION,
            http_options=HttpOptions(api_version="v1"),
        )
        invoke_llm = partial(call_gemini, gemini_client, args.model)
        logging.info(f"Using Gemini model: {args.model}")

    project, dataset, table = (None, None, None)
    if args.table:
        project, dataset, table = args.table.split(".")

    if args.refresh:
        refresh_classifications(bq_client, project, dataset, table, args.model)

    taxonomy = load_taxonomy()
    taxonomy_json = taxonomy_prompt_block(taxonomy)
    logging.info(f"Loaded {len(taxonomy)} taxonomy entries")

    logging.info("Loading Phase 1, ping mapping and probes...")
    phase1 = load_phase1(bq_client, project, dataset, table)
    ping_mapping = load_ping_mapping(bq_client)
    probes_by_ping = load_probes_by_ping(bq_client)
    already_done = get_already_classified(bq_client)
    logging.info(
        f"{len(phase1)} tables to process, {len(already_done)} columns already classified"
    )

    now = datetime.now(timezone.utc).isoformat()

    for (proj, ds, tbl), columns in phase1.items():
        ping_info = ping_mapping.get((proj, ds, tbl), {})
        source_ping = ping_info.get("source_ping")
        ping_platform = ping_info.get("ping_platform")
        ping_probes = (
            probes_by_ping.get((ping_platform, source_ping), []) if source_ping else []
        )

        try:
            descriptions = load_descriptions(bq_client, proj, ds, tbl)
        except Exception as e:
            logging.warning(f"Could not load descriptions for {ds}.{tbl}: {e}")
            descriptions = {}

        # Glean metric leaves (under the undocumented `metrics` STRUCT) are never
        # profiled, so enumerate and match them here for Glean-resolved tables.
        # Each becomes a work item whose matching probes are pre-resolved.
        metric_work = []
        if ping_probes:
            profiled_names = {c["column_name"] for c in columns}
            try:
                metric_columns = load_metric_leaf_columns(bq_client, proj, ds, tbl)
            except Exception as e:
                logging.warning(f"Could not load metric columns for {ds}.{tbl}: {e}")
                metric_columns = []
            for col, matching_probes in match_metric_columns(
                metric_columns, ping_probes, skip_columns=profiled_names
            ):
                col["is_glean_metric"] = True
                metric_work.append((col, matching_probes))

        pending = [
            c
            for c in columns
            if (proj, ds, tbl, c["column_name"], args.model) not in already_done
        ]
        pending_metrics = [
            (col, mp)
            for col, mp in metric_work
            if (proj, ds, tbl, col["column_name"], args.model) not in already_done
        ]
        if not pending and not pending_metrics:
            logging.info(
                f"Skipping {ds}.{tbl} - all {len(columns)} columns already classified"
            )
            continue

        logging.info(
            f"--- {ds}.{tbl} | ping: {source_ping or 'none'} "
            f"| {len(ping_probes)} probes | {len(pending)} columns "
            f"| {len(pending_metrics)} metric columns ---"
        )

        # Each work item is (column dict, pre-resolved matching probes). Profiled
        # columns match probes by their own name; metric columns were matched on
        # their leaf segment above.
        work_items = [
            (col, find_matching_probes(col["column_name"], ping_probes))
            for col in pending
        ] + pending_metrics

        records = []
        for col, matching_probes in work_items:
            col_name = col["column_name"]
            col["bq_description"] = descriptions.get(col_name)
            logging.info(f"  {col_name} → {len(matching_probes)} probe candidates")

            prompt = build_classification_prompt(
                column_name=col_name,
                data_type=col["data_type"],
                table=f"{ds}.{tbl}",
                profile=col,
                matching_probes=matching_probes,
                taxonomy_json=taxonomy_json,
            )

            try:
                result = invoke_llm(prompt)
            except Exception as e:
                logging.error(f"{args.model} call failed for {col_name}: {e}")
                continue

            matched_probe = (
                matching_probes[0]["probe_name"] if matching_probes else None
            )
            probe_sensitivity = (
                matching_probes[0].get("data_sensitivity") if matching_probes else []
            )

            records.append(
                {
                    "source_project": proj,
                    "source_dataset": ds,
                    "source_table": tbl,
                    "column_name": col_name,
                    "data_type": col["data_type"],
                    "primary_label": result.get("primary_label"),
                    "secondary_labels": result.get("secondary_labels") or [],
                    "confidence": result.get("confidence"),
                    "reasoning": result.get("reasoning"),
                    "needs_review": result.get("needs_review"),
                    "matched_probe": matched_probe,
                    "data_sensitivity": probe_sensitivity or [],
                    "model": args.model,
                    "classified_at": now,
                }
            )

        if records:
            save_to_bq(bq_client, records)
            logging.info(f"  Done {ds}.{tbl} ({len(records)} columns)")


if __name__ == "__main__":
    main()
