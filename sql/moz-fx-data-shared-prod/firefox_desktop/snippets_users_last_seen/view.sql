CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_desktop.snippets_users_last_seen`
AS
SELECT
  `moz-fx-data-shared-prod.udf.pos_of_trailing_set_bit`(days_seen_bits) AS days_since_seen,
  *
FROM
  `moz-fx-data-shared-prod.firefox_desktop_derived.snippets_users_last_seen_v2`
