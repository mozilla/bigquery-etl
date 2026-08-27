CREATE TEMP FUNCTION safe_parse_timestamp(ts STRING) AS (
  COALESCE(
        -- full datetime with offset
    SAFE.PARSE_TIMESTAMP("%F%T%Ez", ts),
        -- date + offset (no time)
    SAFE.PARSE_TIMESTAMP("%F%Ez", ts),
        -- datetime with space before offset
    SAFE.PARSE_TIMESTAMP("%F%T%Ez", REGEXP_REPLACE(ts, r"(\+|\-)(\d{2}):(\d{2})", "\\1\\2\\3"))
  )
);

-- sap_aggregates_cte
-- the four grain keys arrive resolved from sap_base_cte, so nothing is re-derived here
SELECT
  client_id,
  submission_date,
  normalized_engine,
  partner_code,
  source,
  MAX(UNIX_DATE(DATE((ping_info.parsed_start_time)))) - MAX(
    UNIX_DATE(DATE(safe_parse_timestamp(client_info.first_run_date)))
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
