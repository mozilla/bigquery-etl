CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.fenix.clients_yearly`
AS
SELECT
  *,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(days_seen_bytes) AS days_since_seen,
  `moz-fx-data-shared-prod`.udf.bits_to_days_since_seen(~days_seen_bytes) AS consecutive_days_seen,
  `moz-fx-data-shared-prod`.udf.bits_to_days_seen(days_seen_bytes) AS days_seen_in_past_year,
  DATE_DIFF(submission_date, first_seen_date, DAY) AS days_since_first_seen,
  EXTRACT(DAYOFWEEK FROM submission_date) AS day_of_week,
FROM
  `moz-fx-data-shared-prod.fenix_derived.clients_yearly_v1`
