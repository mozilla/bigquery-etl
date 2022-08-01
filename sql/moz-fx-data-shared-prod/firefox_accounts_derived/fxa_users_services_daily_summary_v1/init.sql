SELECT
  submission_date,
  service,
  country,
  LANGUAGE,
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
  LANGUAGE,
  app_version,
  os_name,
  os_version
