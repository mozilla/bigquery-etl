CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.telemetry.telemetry_ip_privacy`
AS
SELECT
  DATE(submission_timestamp) AS submission_date,
  *
FROM
  `moz-fx-data-shared-prod.telemetry_derived.telemetry_ip_privacy_v2`
