CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.gecko_trace_aggregates.events`
AS
SELECT
  app_id,
  stable_event_id,
  event_signature,
  source_file,
  source_line,
  result,
  SUM(hit_count) AS hit_count,
  MIN(submission_date) AS first_seen_date,
  MAX(submission_date) AS latest_seen_date
FROM
  (
    SELECT
      "firefox_desktop" AS app_id,
      *
    FROM
      `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_events_v1`
    UNION ALL
    SELECT
      "org_mozilla_fenix_nightly" AS app_id,
      *
    FROM
      `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_events_v1`
    UNION ALL
    SELECT
      "org_mozilla_firefox_beta" AS app_id,
      *
    FROM
      `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_events_v1`
  )
GROUP BY
  app_id,
  stable_event_id,
  event_signature,
  source_file,
  source_line,
  result
