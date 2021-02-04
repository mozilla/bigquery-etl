CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.firefox_accounts.account_ecosystem`
AS SELECT
  * REPLACE(
    mozfun.norm.metadata(metadata) AS metadata)
FROM
  `moz-fx-data-shared-prod.firefox_accounts_stable.account_ecosystem_v1`
