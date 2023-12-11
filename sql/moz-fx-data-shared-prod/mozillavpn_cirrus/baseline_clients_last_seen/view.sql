-- Generated via bigquery_etl.glean_usage
CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.mozillavpn_cirrus.baseline_clients_last_seen`
AS
SELECT
  `moz-fx-data-shared-prod`.udf.pos_of_trailing_set_bit(days_seen_bits) AS days_since_seen,
  `moz-fx-data-shared-prod`.udf.pos_of_trailing_set_bit(
    days_created_profile_bits
  ) AS days_since_created_profile,
  `moz-fx-data-shared-prod`.udf.pos_of_trailing_set_bit(
    days_seen_session_start_bits
  ) AS days_since_seen_session_start,
  `moz-fx-data-shared-prod`.udf.pos_of_trailing_set_bit(
    days_seen_session_end_bits
  ) AS days_since_seen_session_end,
  *
FROM
  `moz-fx-data-shared-prod.mozillavpn_cirrus_derived.baseline_clients_last_seen_v1`
