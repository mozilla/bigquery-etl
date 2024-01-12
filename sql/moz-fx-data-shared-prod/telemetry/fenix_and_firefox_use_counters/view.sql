CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.fenix_and_firefox_use_counters`
AS
SELECT
  submission_date,
  version_major,
  country,
  platform,
  metric,
  rate
FROM
  `mozilla-public-data.firefox_desktop_derived.firefox_desktop_use_counters_v2`
UNION ALL
SELECT
  submission_date,
  version_major,
  country,
  platform,
  metric,
  rate
FROM
  `mozilla-public-data.fenix_derived.fenix_use_counters_v2`
