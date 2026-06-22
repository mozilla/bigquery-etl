"""Side-by-side diff of two model runs of the field classifier.

Reads `mozdata-nonprod.analysis.akomar_field_classifications_v1` (the table
written by `field_classifier.py`), pairs rows by (project, dataset, table,
column) across two `model` values, and prints:

  - overall agreement rate on `primary_label`
  - per-model confidence distribution
  - per-row disagreements with reasoning from both sides

Use `--left` and `--right` to pick the two model names to compare. If
omitted, the script uses the two distinct model values present in the data
(and errors out if there are zero or more than two).
"""

from argparse import ArgumentParser
from collections import Counter

from google.cloud import bigquery

DEST_TABLE = "mozdata-nonprod.analysis.akomar_field_classifications_v1"
DEST_PROJECT = "mozdata-nonprod"


def parse_args():
    """Parse command line arguments."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--table",
        help="Restrict to a single source table (project.dataset.table).",
    )
    parser.add_argument(
        "--left",
        help="Model name for the left-hand column (full name, e.g. claude-sonnet-4-6).",
    )
    parser.add_argument(
        "--right",
        help="Model name for the right-hand column (full name, e.g. gemini-3.1-flash-lite-preview).",
    )
    parser.add_argument(
        "--show-agreements",
        action="store_true",
        help="Also print rows where both models agreed (default: disagreements only).",
    )
    return parser.parse_args()


def _table_filter_clause(table):
    if not table:
        return ""
    project, dataset, source_table = table.split(".")
    return (
        f"WHERE source_project = '{project}' "
        f"AND source_dataset = '{dataset}' "
        f"AND source_table = '{source_table}'"
    )


def resolve_models(bq_client, table, left, right):
    """Resolve --left/--right against models actually present in the data."""
    if left and right:
        return left, right

    where = _table_filter_clause(table)
    query = f"""
        SELECT DISTINCT model
        FROM `{DEST_TABLE}`
        {where}
        ORDER BY model
    """
    found = [r.model for r in bq_client.query(query).result()]
    if len(found) == 0:
        raise SystemExit(
            f"No classification rows found{f' for {table}' if table else ''}."
        )
    if len(found) > 2 and not (left and right):
        raise SystemExit(
            f"Found {len(found)} distinct models ({', '.join(found)}). "
            "Pass --left and --right to pick two."
        )
    if len(found) == 1:
        raise SystemExit(
            f"Only one model present: {found[0]}. Need a second run to compare."
        )
    return found[0], found[1]


def fetch_pairs(bq_client, table, left_model, right_model):
    """Return rows joining left + right model classifications on (table, column)."""
    where = _table_filter_clause(table)

    query = f"""
        WITH base AS (
          SELECT
            source_project, source_dataset, source_table, column_name,
            model,
            primary_label, secondary_labels, confidence, reasoning,
            needs_review, matched_probe, data_sensitivity
          FROM `{DEST_TABLE}`
          {where}
        ),
        l AS (SELECT * FROM base WHERE model = @left_model),
        r AS (SELECT * FROM base WHERE model = @right_model)
        SELECT
          COALESCE(l.source_project, r.source_project) AS source_project,
          COALESCE(l.source_dataset, r.source_dataset) AS source_dataset,
          COALESCE(l.source_table,   r.source_table)   AS source_table,
          COALESCE(l.column_name,    r.column_name)    AS column_name,
          l.primary_label             AS left_label,
          l.confidence                AS left_confidence,
          l.reasoning                 AS left_reasoning,
          l.needs_review              AS left_needs_review,
          l.matched_probe             AS left_matched_probe,
          r.primary_label             AS right_label,
          r.confidence                AS right_confidence,
          r.reasoning                 AS right_reasoning,
          r.needs_review              AS right_needs_review,
          r.matched_probe             AS right_matched_probe
        FROM l
        FULL OUTER JOIN r USING (source_project, source_dataset, source_table, column_name)
        ORDER BY source_dataset, source_table, column_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("left_model", "STRING", left_model),
            bigquery.ScalarQueryParameter("right_model", "STRING", right_model),
        ]
    )
    return list(bq_client.query(query, job_config=job_config).result())


def summarize(pairs):
    """Compute agreement rate and per-model confidence distributions."""
    both = [p for p in pairs if p.left_label and p.right_label]
    only_left = [p for p in pairs if p.left_label and not p.right_label]
    only_right = [p for p in pairs if p.right_label and not p.left_label]
    agree = [p for p in both if p.left_label == p.right_label]
    disagree = [p for p in both if p.left_label != p.right_label]

    return {
        "total_rows": len(pairs),
        "both": len(both),
        "only_left": len(only_left),
        "only_right": len(only_right),
        "agree": len(agree),
        "disagree": len(disagree),
        "left_confidence": Counter(p.left_confidence for p in pairs if p.left_label),
        "right_confidence": Counter(p.right_confidence for p in pairs if p.right_label),
        "agree_rows": agree,
        "disagree_rows": disagree,
    }


def fmt_conf(counter):
    """Format Counter as 'high=12 medium=3 low=1'."""
    if not counter:
        return "(no rows)"
    return " ".join(f"{k}={v}" for k, v in sorted(counter.items(), key=lambda x: -x[1]))


def print_summary(s, left_model, right_model):
    """Print the headline numbers."""
    print("=" * 72)
    print(f"MODEL COMPARISON: {left_model}  vs  {right_model}")
    print("=" * 72)
    print(f"Rows joined:           {s['total_rows']}")
    print(f"  Classified by both:  {s['both']}")
    print(f"  Only {left_model}:  {s['only_left']}")
    print(f"  Only {right_model}: {s['only_right']}")
    print()
    if s["both"]:
        rate = 100.0 * s["agree"] / s["both"]
        print(f"Agreement on primary_label: {s['agree']}/{s['both']} ({rate:.1f}%)")
    print()
    print(f"{left_model} confidence:  {fmt_conf(s['left_confidence'])}")
    print(f"{right_model} confidence: {fmt_conf(s['right_confidence'])}")
    print()


def print_row(p, header, left_model, right_model):
    """Print a single side-by-side comparison row."""
    fq = f"{p.source_dataset}.{p.source_table}.{p.column_name}"
    print("-" * 72)
    print(f"{header}: {fq}")
    print(
        f"  {left_model}: {p.left_label or '<none>'} "
        f"({p.left_confidence or '?'}, "
        f"needs_review={p.left_needs_review}, "
        f"probe={p.left_matched_probe or 'none'})"
    )
    if p.left_reasoning:
        print(f"    {p.left_reasoning}")
    print(
        f"  {right_model}: {p.right_label or '<none>'} "
        f"({p.right_confidence or '?'}, "
        f"needs_review={p.right_needs_review}, "
        f"probe={p.right_matched_probe or 'none'})"
    )
    if p.right_reasoning:
        print(f"    {p.right_reasoning}")


def main():
    """Print the comparison report."""
    args = parse_args()
    bq_client = bigquery.Client(project=DEST_PROJECT)

    left_model, right_model = resolve_models(
        bq_client, args.table, args.left, args.right
    )
    pairs = fetch_pairs(bq_client, args.table, left_model, right_model)
    s = summarize(pairs)
    print_summary(s, left_model, right_model)

    if s["disagree_rows"]:
        print("=" * 72)
        print(f"DISAGREEMENTS ({len(s['disagree_rows'])})")
        print("=" * 72)
        for p in s["disagree_rows"]:
            print_row(p, "DISAGREE", left_model, right_model)
        print()

    if args.show_agreements and s["agree_rows"]:
        print("=" * 72)
        print(f"AGREEMENTS ({len(s['agree_rows'])})")
        print("=" * 72)
        for p in s["agree_rows"]:
            print_row(p, "AGREE", left_model, right_model)


if __name__ == "__main__":
    main()
