CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_delete_events`
AS
SELECT
  submission_timestamp,
  user_id,
  hmac_user_id,
  CAST(NULL AS STRING) AS user_id_unhashed
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_delete_events_v1`
UNION ALL
SELECT
  submission_timestamp,
  user_id,
  hmac_user_id,
  user_id_unhashed
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_delete_events_v2`
