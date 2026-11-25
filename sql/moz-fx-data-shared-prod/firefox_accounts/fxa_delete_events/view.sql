CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_delete_events`
AS
-- Deletions before 2026-11-17 come from the log-based tables.
-- Deletions from that date on come from the accounts DB table.
SELECT
  submission_timestamp,
  user_id,
  hmac_user_id,
  CAST(NULL AS STRING) AS user_id_unhashed
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_delete_events_v1`
WHERE
  submission_timestamp < TIMESTAMP '2026-11-17'
UNION ALL
SELECT
  submission_timestamp,
  user_id,
  hmac_user_id,
  user_id_unhashed
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_delete_events_v2`
WHERE
  submission_timestamp < TIMESTAMP '2026-11-17'
UNION ALL
SELECT
  submission_timestamp,
  user_id,
  hmac_user_id,
  user_id_unhashed
FROM
  `moz-fx-data-shared-prod.accounts_backend_derived.delete_events_accounts_db_v1`
WHERE
  submission_timestamp >= TIMESTAMP '2026-11-17'
