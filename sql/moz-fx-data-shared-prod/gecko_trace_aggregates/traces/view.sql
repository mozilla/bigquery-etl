CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.gecko_trace_aggregates.traces`
AS
SELECT
  "firefox_desktop" AS app_id,
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.gecko_trace_traces_v1`
UNION ALL
SELECT
  "org_mozilla_fenix_nightly" AS app_id,
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_fenix_nightly_derived.gecko_trace_traces_v1`
UNION ALL
SELECT
  "org_mozilla_firefox_beta" AS app_id,
  *
FROM
  `moz-fx-data-shared-prod.org_mozilla_firefox_beta_derived.gecko_trace_traces_v1`
