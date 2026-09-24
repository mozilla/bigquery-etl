CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.gecko_trace_aggregates.trace_events`
AS
SELECT
  "firefox_desktop" AS app_id,
  t.stable_trace_id,
  te.trace_signature,
  te.event_position,
  e.stable_event_id,
  te.event_signature,
  e.source_file,
  e.source_line,
  e.result
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_trace_events_v1` te
JOIN
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_traces_v1` t
  USING (trace_signature)
JOIN
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_events_v1` e
  USING (event_signature)
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS app_id,
  t.stable_trace_id,
  te.trace_signature,
  te.event_position,
  e.stable_event_id,
  te.event_signature,
  e.source_file,
  e.source_line,
  e.result
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_trace_events_v1` te
JOIN
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_traces_v1` t
  USING (trace_signature)
JOIN
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_events_v1` e
  USING (event_signature)
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS app_id,
  t.stable_trace_id,
  te.trace_signature,
  te.event_position,
  e.stable_event_id,
  te.event_signature,
  e.source_file,
  e.source_line,
  e.result
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_trace_events_v1` te
JOIN
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_traces_v1` t
  USING (trace_signature)
JOIN
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_events_v1` e
  USING (event_signature)
