CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.firefox_use_counters`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.firefox_use_counters_v1`
