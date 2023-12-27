CREATE TABLE IF NOT EXISTS
  `moz-fx-data-shared-prod.accounts_db_derived.accounts_aggregates_v1`
PARTITION BY
  submission_date
AS
SELECT
  @submission_date AS submission_date,
  (a.disabledAt IS NULL AND a.emailVerified) AS active,
  (
    t.verified IS NOT NULL
    AND t.enabled IS NOT NULL
    AND t.verified
    AND t.enabled
  ) AS two_factor_auth_enabled,
  (r.enabled IS NOT NULL AND r.enabled) AS recovery_keys_enabled,
  COUNT(*) AS total_accounts
FROM
  `moz-fx-data-shared-prod.accounts_db_external.fxa_accounts_v1` a
LEFT JOIN
  `moz-fx-data-shared-prod.accounts_db_external.fxa_totp_v1` t
USING
  (uid)
LEFT JOIN
  `moz-fx-data-shared-prod.accounts_db_external.fxa_recovery_keys_v1` r
USING
  (uid)
GROUP BY
  submission_date,
  active,
  two_factor_auth_enabled,
  recovery_keys_enabled
