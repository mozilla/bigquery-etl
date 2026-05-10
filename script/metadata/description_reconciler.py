"""Reconcile Phase 1 data-driven descriptions with Phase 2 probe definitions.

For each profiled column, finds candidate probes from the source ping,
then calls Claude to reconcile observed data (Phase 1) with source intent (Phase 2).
For columns with no probe match, fetches the ETL query from DataHub to provide
the actual computation logic (e.g. COUNTIF(is_dau) for a dau column).
Produces a final description and routing hint (global vs dataset-scoped).
"""

import base64
import json
import logging
import os
import re
from argparse import ArgumentParser
from datetime import datetime, timezone

import anthropic
import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)

PHASE1_TABLE = "mozdata-nonprod.analysis.gkabbz_data_profiling_test_v1"
MAPPING_TABLE = "mozdata-nonprod.analysis.gkabbz_metadata_phase2_table_pings_v1"
PROBE_TABLE = "mozdata-nonprod.analysis.gkabbz_metadata_phase2_ping_probes_v1"
DEST_TABLE = "mozdata-nonprod.analysis.gkabbz_metadata_phase3_reconciled_v1"
DEST_PROJECT = "mozdata-nonprod"
CLAUDE_MODEL = "claude-sonnet-4-6"
TOP_N_PROBES = 10

DEST_SCHEMA = [
    bigquery.SchemaField(
        "source_project",
        "STRING",
        mode="REQUIRED",
        description="GCP project of the profiled BQ table.",
    ),
    bigquery.SchemaField(
        "source_dataset",
        "STRING",
        mode="REQUIRED",
        description="BQ dataset of the profiled table.",
    ),
    bigquery.SchemaField(
        "source_table",
        "STRING",
        mode="REQUIRED",
        description="BQ table name of the profiled table.",
    ),
    bigquery.SchemaField(
        "column_name",
        "STRING",
        mode="REQUIRED",
        description="Column name; dot notation for nested STRUCT leaf fields (e.g. client_info.app_channel).",
    ),
    bigquery.SchemaField(
        "data_type",
        "STRING",
        mode="NULLABLE",
        description="BQ data type of the column.",
    ),
    bigquery.SchemaField(
        "source_ping",
        "STRING",
        mode="NULLABLE",
        description="Name of the source telemetry ping, from Phase 2 lineage mapping. Null if no ping found.",
    ),
    bigquery.SchemaField(
        "ping_platform",
        "STRING",
        mode="NULLABLE",
        description="Telemetry platform: 'glean' or 'legacy_telemetry'. Null if no ping found.",
    ),
    bigquery.SchemaField(
        "pass1_description",
        "STRING",
        mode="NULLABLE",
        description="Phase 1 data-driven description based on observed values in the BQ table.",
    ),
    bigquery.SchemaField(
        "matched_probe",
        "STRING",
        mode="NULLABLE",
        description="Probe name Claude identified as the best match for this column. Null if no matching probe found.",
    ),
    bigquery.SchemaField(
        "probe_description",
        "STRING",
        mode="NULLABLE",
        description="Description of the matched probe from the source artifact (Glean Dictionary or stable table schema).",
    ),
    bigquery.SchemaField(
        "final_description",
        "STRING",
        mode="NULLABLE",
        description="Claude-reconciled description combining observed data (Phase 1) with source intent (Phase 2). Falls back to pass1_description on error.",
    ),
    bigquery.SchemaField(
        "contradiction",
        "STRING",
        mode="NULLABLE",
        description="Note flagging any contradiction between the Phase 1 observed behavior and the probe or source definition. Null if no contradiction found.",
    ),
    bigquery.SchemaField(
        "routing_hint",
        "STRING",
        mode="NULLABLE",
        description="Routing recommendation: 'global' (consistent across datasets, candidate for global.yaml), 'dataset' (dataset-specific), or 'unknown'.",
    ),
    bigquery.SchemaField(
        "processed_at",
        "TIMESTAMP",
        mode="REQUIRED",
        description="Timestamp when this column was reconciled.",
    ),
]


GITHUB_REPO = "mozilla/bigquery-etl"
GITHUB_API = "https://api.github.com/repos/{repo}/contents/{path}"


def fetch_github_query(project, dataset, table):
    """Fetch query.sql or query.py from GitHub for a BQ table.

    Tries query.sql first, then query.py. Uses GITHUB_TOKEN env var if set
    (required for private repos; optional for public ones).
    Returns (content, filename) or (None, None) if not found.
    """
    token = os.environ.get("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github.v3+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    for filename in ("query.sql", "query.py"):
        path = f"sql/{project}/{dataset}/{table}/{filename}"
        url = GITHUB_API.format(repo=GITHUB_REPO, path=path)
        try:
            response = requests.get(url, headers=headers, timeout=30)
        except Exception as e:
            logging.warning(f"GitHub request failed for {path}: {e}")
            return None, None
        if response.status_code == 200:
            content = base64.b64decode(response.json()["content"]).decode("utf-8")
            logging.info(f"  Fetched {filename} from GitHub ({len(content)} chars)")
            return content, filename
        elif response.status_code == 404:
            continue
        else:
            logging.warning(f"GitHub API returned {response.status_code} for {path}")
            return None, None

    return None, None


# --- Probe matching ---


def normalize_name(name):
    """Strip all non-alphanumeric characters and lowercase for fuzzy matching."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def find_matching_probes(column_name, probes):
    """Return up to TOP_N_PROBES candidate probes for a column name.

    Scores probes by how closely their name matches the column name:
      3 — exact normalized match
      2 — probe name ends or starts with the column name (e.g. app_name → application.name)
      1 — substring match in either direction
    """
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


# --- Claude reconciler ---


def build_reconcile_prompt(
    column_name,
    data_type,
    table,
    source_ping,
    ping_platform,
    pass1_description,
    matching_probes,
    github_query=None,
    github_filename=None,
):
    """Build a Claude prompt to reconcile Phase 1 and Phase 2 descriptions."""
    ping_info = (
        f"{source_ping} ({ping_platform})"
        if source_ping
        else "unknown (no lineage resolved)"
    )

    if matching_probes:
        probe_lines = []
        for p in matching_probes:
            desc = p["probe_description"] or "(no description)"
            probe_lines.append(
                f"  - {p['probe_name']} ({p['probe_type'] or 'unknown type'}): {desc}"
            )
        source_section = (
            "Candidate probe definitions from the source ping:\n"
            + "\n".join(probe_lines)
        )
    elif github_query:
        source_section = (
            f"No matching probe definitions found.\n\n"
            f"ETL query ({github_filename}) from the bqetl GitHub repo:\n"
            f"```sql\n{github_query}\n```"
        )
    else:
        source_section = "No matching probe definitions found — this column is likely computed or aggregated."

    return (
        "You are a data documentation expert for Mozilla's BigQuery data warehouse.\n\n"
        f"Column: {column_name}\n"
        f"Table: {table}\n"
        f"Data type: {data_type}\n"
        f"Source ping: {ping_info}\n\n"
        f"Phase 1 description (based on observed data values):\n{pass1_description}\n\n"
        f"{source_section}\n\n"
        "Task:\n"
        "1. Identify which probe this column maps to (if any). Probe names may use dots instead of underscores.\n"
        "   If an ETL query is provided, find how this column is computed in the SELECT clause.\n"
        "2. Write a final 1-2 sentence description combining observed data (Phase 1) with source intent.\n"
        "3. Note any contradiction between Phase 1 and the source definition.\n"
        "4. Routing hint: 'global' if this field is generic across Mozilla products,"
        " 'dataset' if specific to this dataset or product, 'unknown' if unclear.\n\n"
        "Respond with a JSON object only (no markdown fences):\n"
        '{"matched_probe": "<probe_name or null>", "final_description": "<description>", '
        '"contradiction": "<note or null>", "routing_hint": "global|dataset|unknown"}'
    )


def call_claude(claude_client, prompt):
    """Call Claude and parse the JSON response."""
    response = claude_client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text.strip()
    # Strip markdown fences if Claude wraps the JSON despite being told not to
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


def reconcile_column(
    claude_client,
    column_name,
    data_type,
    table,
    source_ping,
    ping_platform,
    pass1_description,
    matching_probes,
    github_query=None,
    github_filename=None,
):
    """Reconcile one column and return a result dict, falling back to Phase 1 on error."""
    prompt = build_reconcile_prompt(
        column_name,
        data_type,
        table,
        source_ping,
        ping_platform,
        pass1_description,
        matching_probes,
        github_query,
        github_filename,
    )
    try:
        return call_claude(claude_client, prompt)
    except Exception as e:
        logging.error(f"Claude call failed for {column_name}: {e}")
        return {
            "matched_probe": None,
            "final_description": pass1_description,
            "contradiction": None,
            "routing_hint": "unknown",
        }


# --- BQ loaders ---


def load_phase1(bq_client, project=None, dataset=None, table=None):
    """Load Phase 1 columns grouped by (project, dataset, table).

    Skips undocumented columns (no pass1_description).
    """
    where = "WHERE column_tier != 'undocumented' AND pass1_description IS NOT NULL"
    if project and dataset and table:
        where += f" AND source_project = '{project}' AND source_dataset = '{dataset}' AND source_table = '{table}'"
    query = f"""
        SELECT source_project, source_dataset, source_table, column_name, data_type, pass1_description
        FROM `{PHASE1_TABLE}`
        {where}
        ORDER BY source_dataset, source_table, column_name
    """
    rows_by_table = {}
    for row in bq_client.query(query).result():
        key = (row.source_project, row.source_dataset, row.source_table)
        if key not in rows_by_table:
            rows_by_table[key] = []
        rows_by_table[key].append(
            {
                "column_name": row.column_name,
                "data_type": row.data_type,
                "pass1_description": row.pass1_description,
            }
        )
    return rows_by_table


def load_ping_mapping(bq_client):
    """Load table-to-ping mapping from Phase 2 mapping table."""
    query = f"""
        SELECT source_project, source_dataset, source_table, source_ping, ping_platform
        FROM `{MAPPING_TABLE}`
    """
    mapping = {}
    for row in bq_client.query(query).result():
        key = (row.source_project, row.source_dataset, row.source_table)
        mapping[key] = {
            "source_ping": row.source_ping,
            "ping_platform": row.ping_platform,
        }
    return mapping


def load_probes_by_ping(bq_client):
    """Load all probe definitions from Phase 2, grouped by (ping_platform, source_ping)."""
    query = f"""
        SELECT ping_platform, source_ping, probe_name, probe_description, probe_type
        FROM `{PROBE_TABLE}`
    """
    probes_by_ping = {}
    for row in bq_client.query(query).result():
        key = (row.ping_platform, row.source_ping)
        if key not in probes_by_ping:
            probes_by_ping[key] = []
        probes_by_ping[key].append(
            {
                "probe_name": row.probe_name,
                "probe_description": row.probe_description,
                "probe_type": row.probe_type,
            }
        )
    return probes_by_ping


def get_already_reconciled(bq_client):
    """Return set of (project, dataset, table, column) already in the Phase 3 table."""
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


# --- BQ writer ---


def save_to_bq(bq_client, records):
    """Append reconciled records to the Phase 3 table."""
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
        description="Reconcile Phase 1 and Phase 2 descriptions to produce final field descriptions."
    )
    parser.add_argument(
        "--table",
        help="Fully qualified BQ table (project.dataset.table). If omitted, processes all Phase 1 tables.",
    )
    return parser.parse_args()


def main():
    """Reconcile Phase 1 data descriptions with Phase 2 probe definitions for all profiled tables."""
    args = parse_args()
    bq_client = bigquery.Client(project=DEST_PROJECT)
    claude_client = anthropic.Anthropic()

    project, dataset, table = (None, None, None)
    if args.table:
        project, dataset, table = args.table.split(".")

    logging.info("Loading Phase 1, Phase 2 mapping and probes...")
    phase1 = load_phase1(bq_client, project, dataset, table)
    ping_mapping = load_ping_mapping(bq_client)
    probes_by_ping = load_probes_by_ping(bq_client)
    already_done = get_already_reconciled(bq_client)
    logging.info(
        f"{len(phase1)} tables to process, {len(already_done)} columns already reconciled"
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
                f"Skipping {ds}.{tbl} — all {len(columns)} columns already reconciled"
            )
            continue

        # Fetch GitHub query once per table — used for columns with no probe match
        has_unmatched = any(
            not find_matching_probes(c["column_name"], ping_probes) for c in pending
        )
        github_query, github_filename = None, None
        if has_unmatched:
            logging.info(f"  Fetching query from GitHub for {ds}.{tbl}")
            github_query, github_filename = fetch_github_query(proj, ds, tbl)
            if not github_query:
                logging.info(f"  No GitHub query found for {ds}.{tbl}")

        logging.info(
            f"--- {ds}.{tbl} | ping: {source_ping or 'none'} | {len(ping_probes)} probes | {len(pending)} columns ---"
        )

        records = []
        for col in pending:
            col_name = col["column_name"]
            pass1 = col["pass1_description"]
            matching_probes = find_matching_probes(col_name, ping_probes)

            # Only pass GitHub query for columns with no probe match
            col_github_query = github_query if not matching_probes else None
            col_github_filename = github_filename if not matching_probes else None
            logging.info(
                f"  {col_name} → {len(matching_probes)} probes {'(+GitHub query)' if col_github_query else ''}"
            )

            result = reconcile_column(
                claude_client,
                col_name,
                col["data_type"],
                f"{ds}.{tbl}",
                source_ping,
                ping_platform,
                pass1,
                matching_probes,
                github_query=col_github_query,
                github_filename=col_github_filename,
            )

            matched_probe = result.get("matched_probe")
            probe_desc = None
            if matched_probe:
                probe_match = next(
                    (p for p in ping_probes if p["probe_name"] == matched_probe), None
                )
                probe_desc = probe_match["probe_description"] if probe_match else None

            records.append(
                {
                    "source_project": proj,
                    "source_dataset": ds,
                    "source_table": tbl,
                    "column_name": col_name,
                    "data_type": col["data_type"],
                    "source_ping": source_ping,
                    "ping_platform": ping_platform,
                    "pass1_description": pass1,
                    "matched_probe": matched_probe,
                    "probe_description": probe_desc,
                    "final_description": result.get("final_description"),
                    "contradiction": result.get("contradiction"),
                    "routing_hint": result.get("routing_hint"),
                    "processed_at": now,
                }
            )

        save_to_bq(bq_client, records)
        logging.info(f"  Done {ds}.{tbl} ({len(records)} columns)")


if __name__ == "__main__":
    main()
