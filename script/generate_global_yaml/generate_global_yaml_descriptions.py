#!/usr/bin/env python3
"""Generate descriptions for all repeated columns and write to global_v2.yaml.

Reads from the pre-built BQ staging table
`moz-fx-dev-cbeck-sandbox.bigquery_metadata.column_descriptions_aggregated`
and uses the Claude API to:
  - Synthesize a canonical description from existing descriptions (glean/bqetl sources)
  - Generate a description from query.sql context for columns with no existing description

Checkpointing: results are saved to a .checkpoint.json file after each batch so the
script can be safely interrupted and resumed without reprocessing completed columns.

Usage:
    caffeinate -i python generate_global_yaml_descriptions.py
        [--bq-project moz-fx-dev-cbeck-sandbox]
        [--repo-root /path/to/bigquery-etl]
        [--output path/to/global_v2.yaml]
        [--pilot N]          # only process top N rows (for spot-checking)
        [--reset-checkpoint] # ignore existing checkpoint and start fresh

Prerequisites:
    pip install anthropic google-cloud-bigquery
    gcloud auth application-default login
    ANTHROPIC_API_KEY set in environment
"""

import argparse
import csv
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

import anthropic
import yaml
from google.cloud import bigquery

STAGING_TABLE = (
    "moz-fx-dev-cbeck-sandbox.bigquery_metadata.column_descriptions_aggregated"
)
COLUMN_TABLE_MAP = "moz-fx-dev-cbeck-sandbox.bigquery_metadata.column_table_map"
MODEL = "claude-haiku-4-5-20251001"
BATCH_SIZE = 100
SQL_CONTEXT_LINES = 3  # lines of surrounding context around each column reference


# ---------------------------------------------------------------------------
# Checkpoint helpers
# ---------------------------------------------------------------------------


def load_checkpoint(checkpoint_path: Path) -> dict:
    """Load checkpoint dict keyed by column_name, or return empty dict."""
    if checkpoint_path.is_file():
        with open(checkpoint_path) as f:
            data = json.load(f)
        print(f"Resuming from checkpoint: {len(data)} columns already processed")
        return data
    return {}


def save_checkpoint(checkpoint_path: Path, checkpoint: dict) -> None:
    """Write checkpoint dict to disk atomically."""
    tmp = checkpoint_path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(checkpoint, f)
    tmp.replace(checkpoint_path)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_existing_global_yaml(global_yaml_path: Path) -> dict:
    """Return a dict with existing entries keyed by name (includes aliases)."""
    with open(global_yaml_path) as f:
        data = yaml.safe_load(f)
    existing = {}
    for field in data.get("fields", []):
        existing[field["name"]] = field
        for alias in field.get("aliases", []):
            existing[alias] = field
    return existing


def fetch_staging_rows(bq_client: bigquery.Client, limit: Optional[int]) -> list:
    """Fetch rows from the aggregated staging table."""
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
    SELECT
      column_name,
      data_type,
      type_conflicts,
      dataset_count,
      table_count,
      source,
      descriptions
    FROM `{STAGING_TABLE}`
    WHERE (descriptions IS NOT NULL OR source = 'missing')
      AND table_count >= 50
    ORDER BY dataset_count DESC, column_name ASC
    {limit_clause}
    """
    return list(bq_client.query(query).result())


# ---------------------------------------------------------------------------
# SQL context extraction
# ---------------------------------------------------------------------------


def bulk_fetch_column_table_map(
    column_names: list[str], bq_client: bigquery.Client, top_n: int = 3
) -> dict[str, list[tuple[str, str]]]:
    """Fetch dataset_id/table_name for all column_names in a single BQ query.

    Returns a dict mapping column_name -> [(dataset_id, table_name), ...]
    limited to top_n entries per column.
    """
    if not column_names:
        return {}
    # Use UNNEST to pass all column names in one query
    names_literal = ", ".join(f'"{n}"' for n in column_names)
    query = f"""
    SELECT column_name, dataset_id, table_name
    FROM `{COLUMN_TABLE_MAP}`
    WHERE column_name IN ({names_literal})
    """
    rows = list(bq_client.query(query).result())
    result: dict[str, list[tuple[str, str]]] = {}
    for row in rows:
        result.setdefault(row.column_name, [])
        if len(result[row.column_name]) < top_n:
            result[row.column_name].append((row.dataset_id, row.table_name))
    return result


def extract_sql_context(
    column_name: str, dataset_id: str, table_name: str, repo_root: Path
) -> Optional[str]:
    """Extract relevant lines from a query.sql file for the given column."""
    query_file = (
        repo_root
        / "sql"
        / "moz-fx-data-shared-prod"
        / dataset_id
        / table_name
        / "query.sql"
    )
    if not query_file.is_file():
        return None
    lines = query_file.read_text().splitlines()
    pattern = re.compile(rf"\b{re.escape(column_name)}\b", re.IGNORECASE)
    match_indices = [i for i, line in enumerate(lines) if pattern.search(line)]
    if not match_indices:
        return None
    # If any match is an alias (`<source> AS column_name`), also pull context
    # for the source identifier so Claude knows what the column is derived from.
    alias_pattern = re.compile(
        rf"\b(\w+)\s+AS\s+\b{re.escape(column_name)}\b", re.IGNORECASE
    )
    alias_sources = set()
    for idx in match_indices:
        m = alias_pattern.search(lines[idx])
        if m:
            alias_sources.add(m.group(1).lower())
    for source in alias_sources:
        source_pattern = re.compile(rf"\b{re.escape(source)}\b", re.IGNORECASE)
        match_indices += [
            i for i, line in enumerate(lines) if source_pattern.search(line)
        ]
    included = set()
    for idx in match_indices:
        for j in range(
            max(0, idx - SQL_CONTEXT_LINES),
            min(len(lines), idx + SQL_CONTEXT_LINES + 1),
        ):
            included.add(j)
    excerpt = "\n".join(lines[j] for j in sorted(included))
    return f"-- moz-fx-data-shared-prod.{dataset_id}.{table_name}\n{excerpt}"


# ---------------------------------------------------------------------------
# Claude prompts
# ---------------------------------------------------------------------------


def build_synthesis_prompt(batch: list[dict]) -> str:
    """Build a Claude prompt to synthesize a canonical description from existing ones."""
    lines = []
    for item in batch:
        descs = "\n    ".join(f'- "{d}"' for d in (item["descriptions"] or []))
        lines.append(
            f'column_name: {item["column_name"]}\n'
            f'  data_type: {item["data_type"]}\n'
            f'  source: {item["source"]}\n'
            f"  existing_descriptions:\n    {descs}"
        )
    columns_block = "\n\n".join(lines)
    instruction = (
        "You are writing documentation for a data warehouse. For each column below, "
        "synthesize a single canonical one-sentence description that captures the common "
        "meaning across all existing descriptions. Be concise and precise. Do not add "
        'qualifiers like "Represents" or "This field". Just describe what the value is.'
    )
    return (
        f"{instruction}\n\n"
        "Return ONLY a JSON array with this exact structure (no markdown, no extra text):\n"
        '[{"name": "column_name", "description": "one sentence description"}, ...]\n\n'
        f"Columns:\n{columns_block}"
    )


def build_generation_prompt(batch: list[dict]) -> str:
    """Build a Claude prompt to generate a description for columns with no existing ones."""
    lines = []
    for item in batch:
        sql_block = ""
        if item.get("sql_contexts"):
            sql_snippets = "\n---\n".join(item["sql_contexts"])
            sql_block = f"\n  sql_context:\n{sql_snippets}"
        lines.append(
            f'column_name: {item["column_name"]}\n'
            f'  data_type: {item["data_type"]}\n'
            f'  appears_in: {item["dataset_count"]} datasets, {item["table_count"]} tables'
            f"{sql_block}"
        )
    columns_block = "\n\n".join(lines)
    instruction = (
        "You are writing documentation for a data warehouse. For each column below, "
        "write a concise one-sentence description of what the column value represents. "
        "Use the SQL context (if provided) to understand what data is being stored. "
        'Be precise. Do not use qualifiers like "Represents" or "This field is". '
        "Just describe what the value is."
    )
    return (
        f"{instruction}\n\n"
        "Return ONLY a JSON array with this exact structure (no markdown, no extra text):\n"
        '[{"name": "column_name", "description": "one sentence description"}, ...]\n\n'
        f"Columns:\n{columns_block}"
    )


def call_claude(
    client: anthropic.Anthropic, prompt: str, token_totals: dict
) -> list[dict]:
    """Call Claude with the given prompt and return the parsed JSON response."""
    message = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    token_totals["input"] += message.usage.input_tokens
    token_totals["output"] += message.usage.output_tokens
    first_block = message.content[0]
    if not hasattr(first_block, "text"):
        raise ValueError(f"Unexpected content block type: {type(first_block)}")
    content = first_block.text.strip()  # type: ignore[union-attr]
    content = re.sub(r"^```(?:json)?\s*", "", content)
    content = re.sub(r"\s*```$", "", content)
    return json.loads(content)


# ---------------------------------------------------------------------------
# Batch processing with checkpointing
# ---------------------------------------------------------------------------


def process_batches(
    rows: list,
    existing_global: dict,
    checkpoint: dict,
    checkpoint_path: Path,
    repo_root: Path,
    bq_client: bigquery.Client,
    claude_client: anthropic.Anthropic,
    token_totals: dict,
) -> dict:
    """Process all rows in batches. Returns updated checkpoint dict."""
    synthesis_batch = []
    generation_batch = []
    skipped_global = []
    skipped_checkpoint = []

    for row in rows:
        name = row.column_name
        if name in existing_global:
            skipped_global.append(name)
            continue
        if name in checkpoint:
            skipped_checkpoint.append(name)
            continue
        item = {
            "column_name": name,
            "data_type": row.data_type,
            "type_conflicts": row.type_conflicts,
            "dataset_count": row.dataset_count,
            "table_count": row.table_count,
            "source": row.source,
            "descriptions": list(row.descriptions) if row.descriptions else None,
        }
        if row.source in ("glean", "bqetl"):
            synthesis_batch.append(item)
        else:
            generation_batch.append(item)

    print(f"Skipped {len(skipped_global)} columns already in global.yaml")
    print(f"Skipped {len(skipped_checkpoint)} columns already in checkpoint")
    print(f"Synthesis batch: {len(synthesis_batch)} columns (glean/bqetl sources)")
    print(
        f"Generation batch: {len(generation_batch)} columns (no existing descriptions)"
    )

    total_batches = -(-len(synthesis_batch) // BATCH_SIZE) + -(
        -len(generation_batch) // BATCH_SIZE
    )
    batch_num = 0

    # --- Synthesis batches ---
    for i in range(0, len(synthesis_batch), BATCH_SIZE):
        chunk = synthesis_batch[i : i + BATCH_SIZE]
        batch_num += 1
        print(f"  [{batch_num}/{total_batches}] Synthesizing {len(chunk)} columns...")
        prompt = build_synthesis_prompt(chunk)
        generated = call_claude(claude_client, prompt, token_totals)
        name_to_desc = {item["name"]: item["description"] for item in generated}
        for orig in chunk:
            checkpoint[orig["column_name"]] = {
                **orig,
                "final_description": name_to_desc.get(orig["column_name"], ""),
            }
        save_checkpoint(checkpoint_path, checkpoint)

    # --- Generation batches (with query.sql context) ---
    print("  Fetching query.sql context for generated columns (bulk BQ lookup)...")
    column_table_map = bulk_fetch_column_table_map(
        [item["column_name"] for item in generation_batch], bq_client
    )
    for item in generation_batch:
        tables = column_table_map.get(item["column_name"], [])
        item["sql_contexts"] = [
            ctx
            for dataset_id, table_name in tables
            if (
                ctx := extract_sql_context(
                    item["column_name"], dataset_id, table_name, repo_root
                )
            )
        ]

    for i in range(0, len(generation_batch), BATCH_SIZE):
        chunk = generation_batch[i : i + BATCH_SIZE]
        batch_num += 1
        print(f"  [{batch_num}/{total_batches}] Generating {len(chunk)} columns...")
        prompt = build_generation_prompt(chunk)
        generated = call_claude(claude_client, prompt, token_totals)
        name_to_desc = {item["name"]: item["description"] for item in generated}
        for orig in chunk:
            checkpoint[orig["column_name"]] = {
                **orig,
                "final_description": name_to_desc.get(orig["column_name"], ""),
            }
        save_checkpoint(checkpoint_path, checkpoint)

    return checkpoint


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def write_global_yaml(
    checkpoint: dict,
    existing_global_path: Path,
    output_path: Path,
) -> None:
    """Merge generated fields into the existing global YAML and write to output_path."""
    with open(existing_global_path) as f:
        existing_data = yaml.safe_load(f)

    existing_fields = existing_data.get("fields", [])
    existing_names = {f["name"] for f in existing_fields}

    new_fields = []
    for r in checkpoint.values():
        if r["column_name"] in existing_names:
            continue
        raw_type = r["data_type"] or ""
        if raw_type.startswith("STRUCT<") or raw_type.startswith("ARRAY<STRUCT<"):
            bq_type = "RECORD"
        elif raw_type.startswith("ARRAY<"):
            bq_type = raw_type[6:].rstrip(">")
        else:
            bq_type = raw_type
        new_fields.append(
            {
                "name": r["column_name"],
                "type": bq_type,
                "description": r["final_description"],
            }
        )

    all_fields = sorted(existing_fields + new_fields, key=lambda f: f["name"].lower())

    with open(output_path, "w") as f:
        yaml.dump(
            {"fields": all_fields},
            f,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )

    print(f"Wrote {len(all_fields)} entries to {output_path}")
    print(f"  ({len(existing_fields)} existing + {len(new_fields)} new)")


def _clean(text: str) -> str:
    """Strip newlines and normalize whitespace for safe CSV storage."""
    return " ".join(str(text).split())


def write_csv_log(checkpoint: dict, log_path: Path) -> None:
    """Write a CSV review log of all processed columns and their generated descriptions."""
    with open(log_path, "w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "column_name",
                "data_type",
                "type_conflicts",
                "source",
                "dataset_count",
                "table_count",
                "final_description",
                "source_descriptions",
            ],
            quoting=csv.QUOTE_ALL,
        )
        writer.writeheader()
        for r in sorted(checkpoint.values(), key=lambda x: -x["dataset_count"]):
            writer.writerow(
                {
                    "column_name": r["column_name"],
                    "data_type": r["data_type"],
                    "type_conflicts": r["type_conflicts"],
                    "source": r["source"],
                    "dataset_count": r["dataset_count"],
                    "table_count": r["table_count"],
                    "final_description": _clean(r["final_description"]),
                    "source_descriptions": " | ".join(
                        _clean(d) for d in (r.get("descriptions") or [])
                    ),
                }
            )
    print(f"Wrote review log to {log_path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    """Entry point for the global YAML description generation script."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bq-project", default="moz-fx-dev-cbeck-sandbox")
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Path to the bigquery-etl repo root",
    )
    parser.add_argument(
        "--output", default=None, help="Override output path for global_v2.yaml"
    )
    parser.add_argument(
        "--pilot", type=int, default=None, help="Only process top N rows"
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Ignore existing checkpoint and start fresh",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    global_yaml_path = repo_root / "bigquery_etl" / "schema" / "global.yaml"
    output_path = (
        Path(args.output)
        if args.output
        else repo_root / "bigquery_etl" / "schema" / "global_v2.yaml"
    )
    checkpoint_path = output_path.with_suffix(".checkpoint.json")
    log_path = output_path.with_suffix(".review.csv")

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print(
            "ERROR: ANTHROPIC_API_KEY environment variable is not set.", file=sys.stderr
        )
        sys.exit(1)

    print("Loading existing global.yaml entries...")
    existing_global = load_existing_global_yaml(global_yaml_path)
    print(f"  Found {len(existing_global)} existing names/aliases")

    checkpoint = {}
    if not args.reset_checkpoint:
        checkpoint = load_checkpoint(checkpoint_path)

    print("Connecting to BigQuery...")
    bq_client = bigquery.Client(project=args.bq_project)

    print(
        f"Fetching staging rows{f' (pilot: top {args.pilot})' if args.pilot else ''}..."
    )
    rows = fetch_staging_rows(bq_client, args.pilot)
    print(f"  Fetched {len(rows)} rows")

    claude_client = anthropic.Anthropic(api_key=api_key)
    token_totals = {"input": 0, "output": 0}

    print("Processing batches...")
    checkpoint = process_batches(
        rows,
        existing_global,
        checkpoint,
        checkpoint_path,
        repo_root,
        bq_client,
        claude_client,
        token_totals,
    )
    print(f"  {len(checkpoint)} total columns in checkpoint")
    print(
        f"  Tokens used — input: {token_totals['input']:,}  output: {token_totals['output']:,}  total: {token_totals['input'] + token_totals['output']:,}"
    )

    print("Writing global_v2.yaml...")
    write_global_yaml(checkpoint, global_yaml_path, output_path)

    print("Writing CSV review log...")
    write_csv_log(checkpoint, log_path)

    print("Done.")


if __name__ == "__main__":
    main()
