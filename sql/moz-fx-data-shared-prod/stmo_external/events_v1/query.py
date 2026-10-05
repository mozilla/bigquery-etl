#!/usr/bin/env python3

"""Load a day of Redash events from the STMO Cloud SQL database into events_v1.

This is a script instead of query.sql because EXTERNAL_QUERY only accepts a string
literal, so the date has to be written into the Postgres query before it runs.
"""

from datetime import timedelta

import click
from google.cloud import bigquery

CONNECTION = "moz-fx-data-stmo-prod-33f2.us.stmo-cloudsql-prod"

# created_at isn't indexed, so filtering on it alone scans the whole table.
# ids roughly follow created_at, so this binary searches the primary key for an
# id range around the day, padded by a day on each side for out-of-order rows.
QUERY = '''
SELECT
  id,
  org_id,
  user_id,
  action,
  object_type,
  object_id,
  SAFE.PARSE_JSON(additional_properties, wide_number_mode => 'round')
    AS additional_properties,
  created_at,
FROM
  EXTERNAL_QUERY(
    "{connection}",
    """WITH RECURSIVE search(target, lo, hi) AS (
         SELECT
           target,
           (SELECT min(id) FROM events),
           (SELECT max(id) + 1 FROM events)
         FROM
           unnest(ARRAY[{lower_target}, {upper_target}]) AS target
         UNION ALL
         SELECT
           s.target,
           CASE WHEN probe.created_at < s.target THEN (s.lo + s.hi) / 2 ELSE s.lo END,
           CASE WHEN probe.created_at < s.target THEN s.hi ELSE (s.lo + s.hi) / 2 END
         FROM
           search AS s
         LEFT JOIN LATERAL (
           SELECT created_at FROM events WHERE id >= (s.lo + s.hi) / 2 ORDER BY id LIMIT 1
         ) AS probe ON TRUE
         WHERE
           s.hi - s.lo > 1
       )
       SELECT
         id,
         org_id,
         user_id,
         action,
         object_type,
         object_id,
         additional_properties,
         created_at
       FROM
         events
       WHERE
         id >= (SELECT lo FROM search WHERE target = {lower_target} AND hi - lo <= 1)
         AND id < (SELECT hi FROM search WHERE target = {upper_target} AND hi - lo <= 1)
         AND created_at >= {start}
         AND created_at < {end}
    """
  )
'''


@click.command(help=__doc__)
@click.option(
    "--date",
    "date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="Partition date to load, e.g. 2026-09-30.",
)
@click.option(
    "--billing-project", default="moz-fx-data-shared-prod", help="Billing project."
)
# Required so that a backfill entry with its own query_script_args can't fall back
# to the production table: backfill create only adds the staging destination when
# no query_script_arg is given.
@click.option(
    "--destination-table",
    required=True,
    help="Fully qualified destination table, without a partition decorator.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Dry run the query against the connection without writing the partition.",
)
def main(date, billing_project, destination_table, dry_run):
    """Write one created_at partition of Redash events."""
    date = date.date()
    start = f"TIMESTAMPTZ '{date.isoformat()} 00:00:00+00'"
    end = f"TIMESTAMPTZ '{(date + timedelta(days=1)).isoformat()} 00:00:00+00'"
    query = QUERY.format(
        connection=CONNECTION,
        start=start,
        end=end,
        lower_target=f"{start} - INTERVAL '1 day'",
        upper_target=f"{end} + INTERVAL '1 day'",
    )
    partition = f"{destination_table}${date.strftime('%Y%m%d')}"

    client = bigquery.Client(billing_project)

    if dry_run:
        client.query(query, job_config=bigquery.QueryJobConfig(dry_run=True))
        print(f"Query validates. Skipping write to {partition}.")
        return

    result = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            destination=partition,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    ).result()
    print(f"Wrote {result.total_rows} rows into {partition}")


if __name__ == "__main__":
    main()
