CREATE TEMP FUNCTION safe_parse_timestamp(ts string) AS (
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
SELECT
  client_id,
  DATE(submission_timestamp) AS submission_date,
  CASE
    WHEN JSON_VALUE(event_extra.provider_id) = 'other'
      THEN `moz-fx-data-shared-prod.udf.normalize_search_engine`(
          JSON_VALUE(event_extra.provider_name)
        )
    ELSE `moz-fx-data-shared-prod.udf.normalize_search_engine`(JSON_VALUE(event_extra.provider_id))
  END AS normalized_engine,
  JSON_VALUE(event_extra.partner_code) AS partner_code,
  CASE
    WHEN JSON_VALUE(event_extra.source) = 'urlbar-handoff'
      THEN 'urlbar_handoff'
    ELSE JSON_VALUE(event_extra.source)
  END AS source,
-- end as search_access_point, -- rename
  MAX(UNIX_DATE(DATE((ping_info.parsed_start_time)))) - MAX(
    UNIX_DATE(DATE(safe_parse_timestamp(client_info.first_run_date)))
  ) AS profile_age_in_days,
  COUNT(*) AS sap,
  COUNTIF(ping_info.seq = 1) AS sessions_started_on_this_day,
  SUM(
    CAST(JSON_EXTRACT_SCALAR(metrics.counter, '$.browser_engagement_active_ticks') AS float64) / (
      3600 / 5
    )
  ) AS active_hours_sum,
  SUM(
    CAST(
      JSON_EXTRACT_SCALAR(metrics.counter, '$.browser_engagement_tab_open_event_count') AS float64
    )
  ) AS scalar_parent_browser_engagement_tab_open_event_count_sum,
  SUM(
    CAST(JSON_EXTRACT_SCALAR(metrics.counter, '$.browser_engagement_uri_count') AS float64)
  ) AS scalar_parent_browser_engagement_total_uri_count_sum,
  MAX(
    CAST(
      JSON_EXTRACT_SCALAR(
        metrics.quantity,
        '$.browser_engagement_max_concurrent_tab_count'
      ) AS float64
    )
  ) AS concurrent_tab_count_max
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.events_stream_v1`
WHERE
  DATE(submission_timestamp)
  BETWEEN '2025-06-25'
  AND '2025-09-25'
  AND sample_id = 0
-- date(submission_timestamp) = @submission_date
  AND event = 'sap.counts'
GROUP BY
  client_id,
  submission_date,
  normalized_engine,
  partner_code,
  source
-- search_access_point
