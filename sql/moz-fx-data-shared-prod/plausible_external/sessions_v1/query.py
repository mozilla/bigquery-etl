"""Load one day of Plausible's raw sessions export for firefox.com."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from load import entrypoint, run  # noqa: E402

# Empty strings are normalized to NULL for the same reason as in events_v1: the
# source parquet declares its string columns non-nullable, so "not collected" is
# encoded as "". `entry_props` stays a raw JSON string.
#
# The row_number filter hardens the vendor's own deduplication (DENG-11380 asks
# us to confirm it): sessions.parquet is expected to hold one row per session_id,
# and does in every file inspected, but if a future file repeats one we keep the
# most recently updated row instead of loading both. load.py reports when this
# fires. Written as a CTE plus WHERE rather than QUALIFY, because BigQuery can
# parse a QUALIFY that isn't preceded by WHERE/GROUP BY/HAVING as a table alias.
SELECT_SQL = """
WITH numbered AS (
  SELECT
    CAST(site_id AS INT64) AS site_id,
    CAST(session_id AS NUMERIC) AS session_id,
    `start`,
    `timestamp`,
    NULLIF(hostname, '') AS hostname,
    NULLIF(entry_page, '') AS entry_page,
    NULLIF(exit_page, '') AS exit_page,
    NULLIF(exit_page_hostname, '') AS exit_page_hostname,
    CAST(is_bounce AS BOOL) AS is_bounce,
    CAST(pageviews AS INT64) AS pageviews,
    CAST(event_count AS INT64) AS event_count,
    CAST(duration AS INT64) AS duration,
    NULLIF(referrer, '') AS referrer,
    NULLIF(referrer_source, '') AS referrer_source,
    NULLIF(country_code, '') AS country_code,
    NULLIF(country_name, '') AS country_name,
    NULLIF(subdivision1_code, '') AS subdivision1_code,
    NULLIF(subdivision2_code, '') AS subdivision2_code,
    NULLIF(region_name, '') AS region_name,
    NULLIF(screen_size, '') AS screen_size,
    NULLIF(operating_system, '') AS operating_system,
    NULLIF(operating_system_version, '') AS operating_system_version,
    NULLIF(browser, '') AS browser,
    NULLIF(browser_version, '') AS browser_version,
    NULLIF(utm_medium, '') AS utm_medium,
    NULLIF(utm_source, '') AS utm_source,
    NULLIF(utm_campaign, '') AS utm_campaign,
    NULLIF(utm_content, '') AS utm_content,
    NULLIF(utm_term, '') AS utm_term,
    NULLIF(acquisition_channel, '') AS acquisition_channel,
    entry_props,
    ROW_NUMBER() OVER (
      PARTITION BY session_id
      ORDER BY `timestamp` DESC
    ) AS _session_row
  FROM
    `{tmp_table}`
)
SELECT
  * EXCEPT (_session_row)
FROM
  numbered
WHERE
  _session_row = 1
"""


@entrypoint
def main(args):
    """Load one day. Also the `bqetl query backfill` script entrypoint."""
    run(
        args,
        table="sessions_v1",
        source_file="sessions.parquet",
        # Sessions are filed by when they began, not when they were last
        # updated: each file holds exactly the sessions whose `start` falls on
        # the export date, while `timestamp` can run into the following day.
        # Partitioning on `start` is what keeps one file to one partition.
        partition_field="start",
        select_sql=SELECT_SQL,
        duplicate_key="session_id",
    )


if __name__ == "__main__":
    main()
