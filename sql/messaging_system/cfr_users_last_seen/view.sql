CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.messaging_system.cfr_users_last_seen`
AS
SELECT
  `moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(days_seen_bits) AS days_since_seen,
  `moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(
    days_seen_whats_new_bits
  ) AS days_since_seen_whats_new,
  *
FROM
  `moz-fx-data-shared-prod.messaging_system_derived.cfr_users_last_seen_v1`
