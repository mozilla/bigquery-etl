CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_log_auth_events`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_log_auth_events_v1`
