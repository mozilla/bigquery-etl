"""Classify each profiled column against the Mozilla data taxonomy.

Reads:
  - akomar_data_profiling_v1              (Phase 1: profile + pass1 description)
  - akomar_metadata_phase2_table_pings_v1 (Phase 2: source ping per table)
  - akomar_metadata_phase2_ping_probes_v1 (Phase 2: probes w/ data_sensitivity, tags)
  - classification/taxonomy.json          (preprocessed taxonomy)

Writes:
  - akomar_field_classifications_v1 — one row per column per table
"""

import json
import logging
import re
from argparse import ArgumentParser
from datetime import datetime, timezone
from pathlib import Path

import anthropic
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)

PHASE1_TABLE = "mozdata-nonprod.analysis.akomar_data_profiling_v1"
MAPPING_TABLE = "mozdata-nonprod.analysis.akomar_metadata_phase2_table_pings_v1"
PROBE_TABLE = "mozdata-nonprod.analysis.akomar_metadata_phase2_ping_probes_v1"
DEST_TABLE = "mozdata-nonprod.analysis.akomar_field_classifications_v1"
DEST_PROJECT = "mozdata-nonprod"
CLAUDE_MODEL = "claude-sonnet-4-6"
TAXONOMY_PATH = Path(__file__).parent / "classification" / "taxonomy.json"
TOP_N_PROBES = 3

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
        description="high | medium | low — Claude's self-reported confidence.",
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


# --- Probe matching (lifted from description_reconciler.py, top-3 only) ---


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


def build_classification_prompt(
    column_name, data_type, table, pass1_description, matching_probes, taxonomy_json
):
    """Build a prompt asking Claude to assign a taxonomy label."""
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

    return (
        "You are classifying a BigQuery column against Mozilla's data taxonomy.\n\n"
        f"Table: {table}\n"
        f"Column: {column_name}\n"
        f"Data type: {data_type}\n"
        f"Observed-data description (from profiling): {pass1_description}\n\n"
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


def call_claude(claude_client, prompt):
    """Call Claude and parse the JSON response."""
    response = claude_client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


# --- BQ loaders ---


def load_phase1(bq_client, project=None, dataset=None, table=None):
    """Load Phase 1 columns grouped by (project, dataset, table)."""
    where = "WHERE column_tier != 'undocumented' AND pass1_description IS NOT NULL"
    if project and dataset and table:
        where += (
            f" AND source_project = '{project}'"
            f" AND source_dataset = '{dataset}'"
            f" AND source_table = '{table}'"
        )
    query = f"""
        SELECT source_project, source_dataset, source_table,
               column_name, data_type, pass1_description
        FROM `{PHASE1_TABLE}`
        {where}
        ORDER BY source_dataset, source_table, column_name
    """
    rows_by_table = {}
    for row in bq_client.query(query).result():
        key = (row.source_project, row.source_dataset, row.source_table)
        rows_by_table.setdefault(key, []).append(
            {
                "column_name": row.column_name,
                "data_type": row.data_type,
                "pass1_description": row.pass1_description,
            }
        )
    return rows_by_table


def load_ping_mapping(bq_client):
    """Load table → ping mapping."""
    query = f"""
        SELECT source_project, source_dataset, source_table,
               source_ping, ping_platform
        FROM `{MAPPING_TABLE}`
    """
    mapping = {}
    for row in bq_client.query(query).result():
        mapping[(row.source_project, row.source_dataset, row.source_table)] = {
            "source_ping": row.source_ping,
            "ping_platform": row.ping_platform,
        }
    return mapping


def load_probes_by_ping(bq_client):
    """Load probe definitions grouped by (ping_platform, source_ping)."""
    query = f"""
        SELECT ping_platform, source_ping, probe_name, probe_description,
               probe_type, data_sensitivity, tags
        FROM `{PROBE_TABLE}`
    """
    probes_by_ping = {}
    for row in bq_client.query(query).result():
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
    """Return set of (project, dataset, table, column) already classified."""
    try:
        query = f"""
            SELECT source_project, source_dataset, source_table, column_name
            FROM `{DEST_TABLE}`
        """
        return {
            (r.source_project, r.source_dataset, r.source_table, r.column_name)
            for r in bq_client.query(query).result()
        }
    except NotFound:
        return set()


def save_to_bq(bq_client, records):
    """Append classification records to the destination table."""
    job_config = bigquery.LoadJobConfig(
        schema=DEST_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
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
    return parser.parse_args()


def main():
    """Classify columns from Phase 1 against the Mozilla data taxonomy."""
    args = parse_args()
    bq_client = bigquery.Client(project=DEST_PROJECT)
    claude_client = anthropic.Anthropic()

    project, dataset, table = (None, None, None)
    if args.table:
        project, dataset, table = args.table.split(".")

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

        pending = [
            c for c in columns if (proj, ds, tbl, c["column_name"]) not in already_done
        ]
        if not pending:
            logging.info(
                f"Skipping {ds}.{tbl} — all {len(columns)} columns already classified"
            )
            continue

        logging.info(
            f"--- {ds}.{tbl} | ping: {source_ping or 'none'} "
            f"| {len(ping_probes)} probes | {len(pending)} columns ---"
        )

        records = []
        for col in pending:
            col_name = col["column_name"]
            matching_probes = find_matching_probes(col_name, ping_probes)
            logging.info(f"  {col_name} → {len(matching_probes)} probe candidates")

            prompt = build_classification_prompt(
                column_name=col_name,
                data_type=col["data_type"],
                table=f"{ds}.{tbl}",
                pass1_description=col["pass1_description"],
                matching_probes=matching_probes,
                taxonomy_json=taxonomy_json,
            )

            try:
                result = call_claude(claude_client, prompt)
            except Exception as e:
                logging.error(f"Claude call failed for {col_name}: {e}")
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
                    "classified_at": now,
                }
            )

        if records:
            save_to_bq(bq_client, records)
            logging.info(f"  Done {ds}.{tbl} ({len(records)} columns)")


if __name__ == "__main__":
    main()
