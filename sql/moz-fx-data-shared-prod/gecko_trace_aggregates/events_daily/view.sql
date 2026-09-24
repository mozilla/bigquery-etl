CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.gecko_trace_aggregates.events_daily`
AS
SELECT
  "firefox_desktop" AS app_id,
  d.submission_date,
  e.stable_event_id,
  d.event_signature,
  d.hit_count
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_events_daily_v1` d
JOIN
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_events_v1` e
  USING (event_signature)
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS app_id,
  d.submission_date,
  e.stable_event_id,
  d.event_signature,
  d.hit_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_events_daily_v1` d
JOIN
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_events_v1` e
  USING (event_signature)
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS app_id,
  d.submission_date,
  e.stable_event_id,
  d.event_signature,
  d.hit_count
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_events_daily_v1` d
JOIN
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_events_v1` e
  USING (event_signature)
