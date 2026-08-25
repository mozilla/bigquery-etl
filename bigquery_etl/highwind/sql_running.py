"""SECTION 3: SQL RUNNING.

Executes the per-source queries and returns the sufficient statistics keyed by slug and cell.
Records what each query cost, because on a shared slot reservation wall time reflects how many slots
happened to be free, and slot-hours is the portable number.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery

COHORT_DATASET = "mozdata.tmp"


def materialize_cohort(client, sql, as_of, dry_run=False):
    """Write the run's cohort to its own table and return where it went.

    One table for every experiment in the run, keyed by slug. Materialized rather than inlined
    because all three source queries need it, and a CTE would be re-scanned once per source. The
    assignment scan is a large share of the job's bytes, so paying it once rather than once per
    source per experiment is the difference between a viable daily run and an unaffordable one.
    """
    table = f"{COHORT_DATASET}.highwind_cohort_{as_of.isoformat().replace('-', '_')}"
    started = time.time()
    if dry_run:
        # Validate the cohort query itself, but hand back None so the source queries validate
        # against an inline stub rather than a table this run is not going to create.
        job = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True))
        return None, query_timing("cohort", job, [], time.time() - started)
    # Past the early return above, so this path is never a dry run.
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            destination=table,
            write_disposition="WRITE_TRUNCATE",
            use_query_cache=False,
        ),
    )
    job.result()
    return table, query_timing("cohort", job, [], time.time() - started)


def run_queries(client, queries, dry_run=False):
    """Run every source query, returning (cells_by_slug, timings).

    The queries cover every experiment at once, so the rows come back carrying a slug and are split
    here. `cells_by_slug[slug][(metric, window, branch)]` is that cell's six sufficient statistics,
    which is the shape the statistics section asks for: one comparison needs two branches of one
    (metric, window) for one experiment, and nothing else.

    Run concurrently. The sources are independent tables, so serialising them would make the run's
    wall time their sum rather than the longest of them.
    """
    cells_by_slug, timings = {}, []
    with ThreadPoolExecutor(max_workers=len(queries) or 1) as pool:
        futures = {
            pool.submit(run_one_query, client, source, sql, dry_run): source
            for source, sql in queries.items()
        }
        for future in as_completed(futures):
            rows, timing = future.result()
            timings.append(timing)
            for row in rows:
                cells_by_slug.setdefault(row["slug"], {})[
                    (row["metric"], row["window_label"], row["branch"])
                ] = sufficient_stats(row)
    return cells_by_slug, sorted(timings, key=lambda t: t["source"])


def run_one_query(client, source, sql, dry_run):
    """Execute one source query and report what it consumed."""
    started = time.time()
    job = client.query(
        sql, job_config=bigquery.QueryJobConfig(dry_run=dry_run, use_query_cache=False)
    )
    rows = [] if dry_run else [dict(row) for row in job.result()]
    return rows, query_timing(source, job, rows, time.time() - started)


def query_timing(source, job, rows, wall_seconds):
    """Report what the query cost, in the terms that survive a shared reservation."""
    slot_millis = getattr(job, "slot_millis", None)
    return dict(
        source=source,
        wall_s=round(wall_seconds, 1),
        gb_scanned=round((job.total_bytes_processed or 0) / 1e9, 1),
        slot_hours=None if slot_millis is None else round(slot_millis / 3_600_000, 2),
        rows=len(rows),
    )


def sufficient_stats(row):
    """One cell's six aggregates, as floats with an integer count."""
    return {
        "n": int(row["n"]),
        "sum": float(row["sum"] or 0.0),
        "sumsq": float(row["sumsq"] or 0.0),
        "pre_sum": float(row["pre_sum"] or 0.0),
        "pre_sumsq": float(row["pre_sumsq"] or 0.0),
        "xp": float(row["xp"] or 0.0),
    }
