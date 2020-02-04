#!/usr/bin/env python3
"""Experiment enrollment aggregates live view query generator."""
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
    """Create a BigQuery SQL query for the experiment enrollments aggregates view with new date filled in.
       Unfortunately, BigQuery does not allow parameters in view definitions, so this script is a very thin
       wrapper over the nearly complete sql query to fill in the cutoff date for using the pre-aggregated vs
       live table for the view"""
    query = textwrap.dedent(
        """
        CREATE
        OR REPLACE VIEW `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_live` AS
          WITH live AS (
            SELECT
              event_object AS `type`,
              event_string_value AS experiment,
              `moz-fx-data-shared-prod`.udf.get_key(event_map_values, 'branch') AS branch,
              TIMESTAMP_ADD(
                TIMESTAMP_TRUNC(`timestamp`, HOUR),
                -- Aggregates event counts over 5-minute intervals
                INTERVAL (DIV(EXTRACT(MINUTE FROM `timestamp`), 5) * 5) MINUTE
              ) AS window_start,
              TIMESTAMP_ADD(
                TIMESTAMP_TRUNC(`timestamp`, HOUR),
                INTERVAL ((DIV(EXTRACT(MINUTE FROM `timestamp`), 5) + 1) * 5) MINUTE
              ) AS window_end,
              COUNTIF(event_method = 'enroll') AS enroll_count,
              COUNTIF(event_method = 'unenroll') AS unenroll_count,
              COUNTIF(event_method = 'graduate') AS graduate_count,
              COUNTIF(event_method = 'update') AS update_count,
              COUNTIF(event_method = 'enrollFailed') AS enroll_failed_count,
              COUNTIF(event_method = 'unenrollFailed') AS unenroll_failed_count,
              COUNTIF(event_method = 'updateFailed') AS update_failed_count
            FROM
              `moz-fx-data-shared-prod.telemetry_derived.events_live`
            WHERE
              event_category = 'normandy'
              AND date(timestamp) > '{submission_date}'
          GROUP BY
            `type`,
            experiment,
            branch,
            window_start,
            window_end
        ),
        previous AS (
          SELECT
            *
          FROM
            `moz-fx-data-shared-prod.telemetry_derived.experiment_enrollment_aggregates_v1`
          WHERE
            date(window_start) <= '{submission_date}'
        ),
        all_enrollments AS (
          SELECT
            *
          FROM
            previous
          UNION ALL
          SELECT
            *
          FROM
            live
        )
        SELECT
          *,
          SUM(enroll_count) OVER previous_rows_window AS cumulative_enroll_count,
          SUM(unenroll_count) OVER previous_rows_window AS cumulative_unenroll_count,
          SUM(graduate_count) OVER previous_rows_window AS cumulative_graduate_count,
          SUM(update_count) OVER previous_rows_window AS cumulative_update_count,
          SUM(enroll_failed_count) OVER previous_rows_window AS cumulative_enroll_failed_count,
          SUM(unenroll_failed_count) OVER previous_rows_window AS cumulative_unenroll_failed_count,
          SUM(update_failed_count) OVER previous_rows_window AS cumulative_update_failed_count
        FROM
          all_enrollments
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
    if opts['json_output']:
        return json.dumps(query)
    else:
        return query


def main(argv, out=print):
    """Print experiment enrollments aggregates view sql to stdout."""
    opts = vars(p.parse_args(argv[1:]))
    sleep(opts['wait_seconds'])
    out(generate_sql(opts))


if __name__ == "__main__":
    main(sys.argv)
