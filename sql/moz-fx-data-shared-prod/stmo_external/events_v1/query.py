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
# additional_properties is parsed here instead of in Postgres because Postgres 9.6 has no
# safe JSON cast, so one malformed row would fail the whole day. Only the listed properties are
# kept. The others include IP addresses, query text, and search terms. referer is cut to its
# origin, since public dashboard URLs contain a share token. api_key is a label such as
# "ApiKey: Query 123", not the key.
QUERY = '''
SELECT
  id,
  org_id,
  user_id,
  action,
  object_type,
  object_id,
  created_at,
  IF(
    properties IS NULL,
    NULL,
    JSON_STRIP_NULLS(
      JSON_OBJECT(
        -- Set when a request used a query or dashboard API key instead of a login, which is why
        -- embed and public dashboard views have no user_id
        'api_key', properties.api_key,
        -- "hit" or "miss" on execute_query, whether the result came from the cache
        'cache', properties.cache,
        -- true on the query and visualization views from a dashboard widget, which tells them
        -- apart from query page loads
        'dashboard', properties.dashboard,
        'embed', properties.embed,
        -- Download format on api_get, e.g. csv or json
        'file_type', properties.file_type,
        -- Set on public dashboard views, true when opened without the Redash header
        'headless', properties.headless,
        'public', properties.public,
        'query_id', properties.query_id,
        -- Site an embed or public dashboard was loaded from, e.g. Confluence
        'referer', TO_JSON(REGEXP_EXTRACT(LAX_STRING(properties.referer), r'^[a-zA-Z]+://[^/]+')),
        -- Only on events logged by the web page, not the server. A dashboard open logs one of
        -- each, so this picks out one event per open.
        'screen_resolution', properties.screen_resolution,
        -- Tells browsers apart from scripts using the API, bots, and headless browsers
        'user_agent', properties.user_agent
      )
    )
  ) AS additional_properties,
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
         created_at,
         additional_properties
       FROM
         events
       WHERE
         id >= (SELECT lo FROM search WHERE target = {lower_target} AND hi - lo <= 1)
         AND id < (SELECT hi FROM search WHERE target = {upper_target} AND hi - lo <= 1)
         AND created_at >= {start}
         AND created_at < {end}
    """
  ) AS events,
  UNNEST([SAFE.PARSE_JSON(events.additional_properties, wide_number_mode => 'round')]) AS properties
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
