CREATE OR REPLACE TABLE
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_daily_aggregates_v1`
PARTITION BY
  submission_date
CLUSTER BY
  service,
  country,
  os_name
OPTIONS
  (require_partition_filter = TRUE)
AS
SELECT
  submission_date,
  service,
  country,
  language,
  app_version,
  os_name,
  os_version,
  COUNT(DISTINCT user_id) AS service_users,
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_daily_v1`
GROUP BY
  submission_date,
  service,
  country,
  language,
  app_version,
  os_name,
  os_version
