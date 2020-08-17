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
    required=True
)
p.add_argument(
    "--json-output",
    action='store_true',
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
        CREATE
        OR REPLACE VIEW `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_live_v1` AS
          WITH all_experiments_searches_live AS (
            SELECT
              submission_timestamp AS timestamp,
              unnested_experiments,
              unnested_ad_clicks,
              unnested_search_with_ads
              unnested_search_counts
            FROM
              `moz-fx-data-shared-prod.telemetry_live.main_v4`,
            UNNEST(environment.experiments) AS unnested_experiments,
            UNNEST(payload.processes.parent.keyed_scalars.browser_search_ad_clicks) AS unnested_ad_clicks,
            UNNEST(payload.processes.parent.keyed_scalars.browser_search_with_ads) AS unnested_search_with_ads,
            UNNEST(ARRAY(
              SELECT AS STRUCT
                SUBSTR(_key, 0, pos - 2) AS engine,
                SUBSTR(_key, pos) AS source,
                udf.extract_histogram_sum(value) AS `count`
              FROM
                UNNEST(payload.keyed_histograms.search_counts),
                UNNEST([REPLACE(key, 'in-content.', 'in-content:')]) AS _key,
                UNNEST([LENGTH(REGEXP_EXTRACT(_key, '.+[.].'))]) AS pos
            )) AS search_counts
            WHERE
              date(submission_timestamp) > '{submission_date}'
              AND ARRAY_LENGTH(environment.experiments) > 0
          ),
          live AS (
            SELECT
              unnested_experiments.key AS experiment,
              unnested_experiments.value AS branch,
              TIMESTAMP_ADD(
                TIMESTAMP_TRUNC(`timestamp`, HOUR),
                -- Aggregates event counts over 5-minute intervals
                INTERVAL(DIV(EXTRACT(MINUTE FROM `timestamp`), 5) * 5) MINUTE
              ) AS window_start,
              TIMESTAMP_ADD(
                TIMESTAMP_TRUNC(`timestamp`, HOUR),
                INTERVAL((DIV(EXTRACT(MINUTE FROM `timestamp`), 5) + 1) * 5) MINUTE
              ) AS window_end,
              SUM(unnested_ad_clicks.value) AS ad_clicks_count,
              SUM(unnested_search_with_ads.value) AS search_with_ads_count,
              SUM(unnested_search_counts.count) AS search_counts
            FROM
              all_experiments_searches_live
            GROUP BY
              experiment,
              branch,
              window_start,
              window_end
        ),
        previous AS (
          SELECT
            *
          FROM
            `moz-fx-data-shared-prod.telemetry_derived.experiment_search_aggregates_v1`
          WHERE
            date(window_start) <= '{submission_date}'
        )
        SELECT
          *
        FROM
          previous
        UNION ALL
        SELECT
          *
        FROM
          live
        """.format(
            **opts
        )
    )
    if opts['json_output']:
        return json.dumps(query)
    else:
        return query


def main(argv, out=print):
    """Print experiment search aggregates view sql to stdout."""
    opts = vars(p.parse_args(argv[1:]))
    sleep(opts['wait_seconds'])
    out(generate_sql(opts))


if __name__ == "__main__":
    main(sys.argv)
