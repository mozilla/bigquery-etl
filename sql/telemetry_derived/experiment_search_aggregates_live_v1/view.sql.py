#!/usr/bin/env python3
"""Experiment search aggregates live view query generator."""
import argparse
import datetime
import json
import sys
import textwrap
from time import sleep

p = argparse.ArgumentParser()
p.add_argument(
    "--submission-date",
    type=str,
    help="Cut-off date for using pre-computed vs live tables in view",
    required=True,
)
p.add_argument(
    "--json-output",
    action="store_true",
    help="Output the result wrapped in json parseable as an XCOM",
)
p.add_argument(
    "--wait-seconds",
    type=int,
    default=0,
    help="Add a delay before executing the script to allow time for the xcom sidecar to complete startup",
)


def generate_sql(opts):
    """Create a BigQuery SQL query for the experiment search aggregates view with new date filled in.
       Unfortunately, BigQuery does not allow parameters in view definitions, so this script is a very thin
       wrapper over the nearly complete sql query to fill in the cutoff date for using the pre-aggregated vs
       live table for the view"""
    query = textwrap.dedent(
        """
        CREATE OR REPLACE VIEW
          `moz-fx-data-shared-prod.telemetry.experiment_search_aggregates_live_v1`
        AS
        WITH all_searches AS (
          SELECT
            *
          FROM
            `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_v1`
          WHERE
            date(window_start) <= DATE '{submission_date}'
          UNION ALL
          SELECT
            * EXCEPT (submission_date, dataset_id)
          FROM
            `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_base`
          WHERE
            submission_date > DATE '{submission_date}'
            AND dataset_id = 'telemetry_live'
        )
        SELECT
          *,
          SUM(search_count) OVER previous_rows_window AS cumulative_search_count,
          SUM(search_with_ads_count) OVER previous_rows_window AS cumulative_search_with_ads_count,
          SUM(ad_clicks_count) OVER previous_rows_window AS cumulative_ad_clicks_count
        FROM
          all_searches
        WINDOW previous_rows_window AS (
          PARTITION BY
            experiment,
            branch
          ORDER BY
            window_start
          ROWS BETWEEN
            UNBOUNDED PRECEDING
            AND CURRENT ROW
        )
        """.format(
            **opts
        )
    )
    if opts["json_output"]:
        return json.dumps(query)
    else:
        return query


def main(argv, out=print):
    """Print experiment search aggregates view sql to stdout."""
    opts = vars(p.parse_args(argv[1:]))
    sleep(opts["wait_seconds"])
    out(generate_sql(opts))


if __name__ == "__main__":
    main(sys.argv)
