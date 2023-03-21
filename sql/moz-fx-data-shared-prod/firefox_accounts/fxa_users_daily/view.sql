CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.fxa_users_daily`
AS
SELECT
  *
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_daily_v1`
