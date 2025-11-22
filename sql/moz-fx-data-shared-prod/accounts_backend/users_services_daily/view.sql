CREATE OR REPLACE VIEW
  `moz-fx-data-shared-prod.accounts_backend.users_services_daily`
AS
SELECT
  submission_date,
  user_id_sha256,
  service,
  country,
  registered,
  seen_in_tier1_country,
  user_agent_device_count,
FROM
  `moz-fx-data-shared-prod.accounts_backend_derived.users_services_daily_v1`
WHERE
  submission_date >= "2024-01-01"
-- FxA logging has been migrated over to GLEAN from Cloud logging,
-- Below is needed to include FxA user entries prior to the migration.
UNION ALL
SELECT
  submission_date,
  user_id AS user_id_sha256,
  service,
  country,
  registered,
  seen_in_tier1_country,
  NULL AS user_agent_device_count,
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_daily_v2`
WHERE
  submission_date < "2024-01-01"