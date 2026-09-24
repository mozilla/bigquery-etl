CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.gecko_trace_aggregates.traces_daily`
AS
SELECT
  "firefox_desktop" AS app_id,
  d.submission_date,
  t.stable_trace_id,
  d.trace_signature,
  d.hit_count,
  d.avg_duration_nano
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_traces_daily_v1` d
JOIN
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_traces_v1` t
  USING (trace_signature)
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS app_id,
  d.submission_date,
  t.stable_trace_id,
  d.trace_signature,
  d.hit_count,
  d.avg_duration_nano
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_traces_daily_v1` d
JOIN
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_traces_v1` t
  USING (trace_signature)
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS app_id,
  d.submission_date,
  t.stable_trace_id,
  d.trace_signature,
  d.hit_count,
  d.avg_duration_nano
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_traces_daily_v1` d
JOIN
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_traces_v1` t
  USING (trace_signature)
