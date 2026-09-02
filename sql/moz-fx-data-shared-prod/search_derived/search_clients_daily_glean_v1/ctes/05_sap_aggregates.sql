-- the date portion of a client-local timestamp string. Every value it reads carries a trailing
-- offset, so the date is its first ten characters. Converting to UTC would shift it a day for
-- part of the world, and profile_age_in_days subtracts two of these, so both terms have to be on
-- the same calendar. Parsing no time also absorbs the shapes that carry no seconds component.
CREATE TEMP FUNCTION local_date_of(ts STRING) AS (
  SAFE.PARSE_DATE('%F', SUBSTR(ts, 1, 10))
);

-- sap_aggregates_cte
-- the four grain keys arrive resolved from sap_base_cte, so nothing is re-derived here
SELECT
  client_id,
  submission_date,
  normalized_engine,
  partner_code,
  source,
  -- start_time, not parsed_start_time: same instant, but DATE() of a TIMESTAMP is a UTC date,
  -- which would put this term on a different calendar from the subtrahend
  MAX(UNIX_DATE(local_date_of(ping_info.start_time))) - MAX(
    UNIX_DATE(local_date_of(client_info.first_run_date))
  ) AS profile_age_in_days,
  COUNT(*) AS sap_counts_total,
  MAX(
    CAST(
      JSON_EXTRACT_SCALAR(
        metrics.quantity,
        '$.browser_engagement_max_concurrent_tab_count'
      ) AS float64
    )
  ) AS concurrent_tab_count_max
FROM
  `search_derived.search_clients_daily_glean_v1.sap_base_cte`
GROUP BY
  client_id,
  submission_date,
  normalized_engine,
  partner_code,
  source
