CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_users_first_seen`
AS
SELECT
  *,
  -- TODO: remove the `services_used` field once we confirm no downstream usage.
  [CAST(NULL AS STRING)] AS services_used,
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_first_seen_v2`
