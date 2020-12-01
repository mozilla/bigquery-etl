CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.telemetry_derived.events_last_seen_v1`
PARTITION BY
  submission_date
CLUSTER BY
  sample_id
AS
SELECT
  submission_date,
  sample_id,
  client_id,
  CAST(FALSE AS INT64) AS days_logged_event_bits,
  CAST(FALSE AS INT64) AS days_viewed_protection_report_bits,
  CAST(FALSE AS INT64) AS days_used_pip_bits,
  CAST(FALSE AS INT64) AS days_had_cert_error_bits,
FROM
  telemetry.events
WHERE
  FALSE
