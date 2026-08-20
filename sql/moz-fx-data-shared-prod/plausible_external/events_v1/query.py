"""Load one day of Plausible's raw events export for firefox.com."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from load import parse_args, run  # noqa: E402

# Empty strings are normalized to NULL: the source parquet declares every string
# column non-nullable, so Plausible encodes "not collected" as "" (72% of
# subdivision2_code, 79% of referrer). Without this, every downstream filter
# would need its own NULLIF.
#
# `props` is deliberately left as the raw JSON string rather than parsed into
# BigQuery's JSON type, so a malformed or newly-shaped value from the vendor can
# never fail this load. Downstream should use JSON_VALUE/PARSE_JSON.
SELECT_SQL = """
SELECT
  CAST(site_id AS INT64) AS site_id,
  CAST(session_id AS NUMERIC) AS session_id,
  `timestamp`,
  NULLIF(`name`, '') AS `name`,
  NULLIF(hostname, '') AS hostname,
  NULLIF(pathname, '') AS pathname,
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
  CAST(scroll_depth AS INT64) AS scroll_depth,
  CAST(engagement_time AS INT64) AS engagement_time,
  CAST(revenue_reporting_amount AS NUMERIC) AS revenue_reporting_amount,
  NULLIF(revenue_reporting_currency, '') AS revenue_reporting_currency,
  CAST(revenue_source_amount AS NUMERIC) AS revenue_source_amount,
  NULLIF(revenue_source_currency, '') AS revenue_source_currency,
  props
FROM
  `{tmp_table}`
"""


def main():
    """Load one day. Also the `bqetl query backfill` script entrypoint."""
    run(
        parse_args(__doc__),
        table="events_v1",
        source_file="events.parquet",
        # Each events file holds exactly the events recorded on the export date,
        # so `timestamp` is both the partition column and the check that the
        # file lines up with the partition being replaced.
        partition_field="timestamp",
        select_sql=SELECT_SQL,
    )


if __name__ == "__main__":
    main()
