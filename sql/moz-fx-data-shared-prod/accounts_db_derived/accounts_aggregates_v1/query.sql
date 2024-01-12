-- Source tables of this query aren't partitioned and are overwritten daily.
-- To make this query idempotent we need to make sure that the data for the
-- current submission_date is not overwritten. If data for the current
-- submission_date is already present in the table, this query returns the
-- existing data. Otherwise, it returns the new data aggregated from the
-- source tables.
WITH current_data AS (
  SELECT
    submission_date,
    active,
    two_factor_auth_enabled,
    recovery_keys_enabled,
    total_accounts,
  FROM
    `moz-fx-data-shared-prod.accounts_db_derived.accounts_aggregates_v1`
  WHERE
    submission_date = @submission_date
),
new_data AS (
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
    USING (uid)
  LEFT JOIN
    `moz-fx-data-shared-prod.accounts_db_external.fxa_recovery_keys_v1` r
    USING (uid)
  GROUP BY
    submission_date,
    active,
    two_factor_auth_enabled,
    recovery_keys_enabled
)
SELECT
  submission_date,
  active,
  two_factor_auth_enabled,
  recovery_keys_enabled,
  total_accounts,
FROM
  current_data
UNION ALL
SELECT
  submission_date,
  active,
  two_factor_auth_enabled,
  recovery_keys_enabled,
  total_accounts,
FROM
  new_data
WHERE
  NOT EXISTS(SELECT * FROM current_data WHERE submission_date = @submission_date)
