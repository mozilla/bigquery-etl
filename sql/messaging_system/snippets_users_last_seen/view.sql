CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.messaging_system.snippets_users_last_seen`
AS
SELECT
  `moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(days_seen_bits) AS days_since_seen,
  *
FROM
  `moz-fx-data-shared-prod.messaging_system_derived.snippets_users_last_seen_v1`
