CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.use_counters`
AS 
SELECT
  submission_date,
  version_major,
  country,
  platform,
  metric,
  rate
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.use_counters_v1`
UNION ALL
SELECT
  submission_date,
  version_major,
  country,
  platform,
  metric,
  rate
FROM
  `moz-fx-data-shared-prod.fenix_derived.use_counters_v1`
