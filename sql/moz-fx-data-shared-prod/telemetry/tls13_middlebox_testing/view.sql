CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.tls13_middlebox_testing`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.telemetry_stable.tls13_middlebox_testing_v4`
