CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_delete_events`
AS
SELECT
  submission_timestamp,
  user_id,
  hmac_user_id
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_delete_events_v1`
UNION ALL
SELECT
  submission_timestamp,
  user_id,
  hmac_user_id
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_delete_events_v2`
