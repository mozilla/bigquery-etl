"""Run the Highwind analysis for one date.

A thin entry point. Everything it calls lives in `bigquery_etl.highwind`, because the analysis is
several modules rather than one query and the directory a bigquery-etl job lives in holds a single
script. See that package for the flow.

This job writes three things, only one of which is the table it is named for:

    highwind_statistics_v1        one row per (slug, metric, window, comparison), the results
    highwind_sufficient_stats_v1  one row per (slug, metric, window, branch), what they came from
    gs://mozanalysis/highwind/<slug>.json   per-experiment results, the seam Experimenter reads

Writing more than its own table is unusual, and it is why the job is a single directory rather than
one per table: all three outputs come from the same computation, and splitting them would mean
running that computation twice. The enrollment funnel in this same dataset sets the precedent for a
query.py whose real output is a GCS object.
"""

import datetime
from argparse import ArgumentParser

from bigquery_etl.highwind import run_daily_job, systemic_failure

parser = ArgumentParser(description=__doc__)
parser.add_argument("--date", required=True, help="run date, YYYY-MM-DD")
parser.add_argument(
    "--workers",
    type=int,
    default=8,
    help=(
        "experiments whose statistics are computed concurrently. The BigQuery scan is shared "
        "across all experiments and does not depend on this."
    ),
)


def main():
    """Analyse every live Firefox Desktop experiment for the run date."""
    args = parser.parse_args()
    outcomes = run_daily_job(
        as_of=datetime.date.fromisoformat(args.date),
        workers=args.workers,
        live_only=True,
    )
    # A run where nothing succeeded must not exit zero. Under Airflow that is a green task that did
    # nothing, which is the failure mode hardest to notice and slowest to diagnose.
    if systemic_failure(outcomes):
        raise SystemExit("Highwind produced no results for any experiment")


if __name__ == "__main__":
    main()
