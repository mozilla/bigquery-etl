CREATE MATERIALIZED VIEW `moz-fx-data-shared-prod.telemetry_derived.test_materialized_view`
OPTIONS
  (enable_refresh = FALSE)
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.static.country_codes_v1`
