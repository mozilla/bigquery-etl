SELECT
  submission_date,
  user_id,
  country,
  `language`,
  os_name,
  os_version,
  LOGICAL_OR(seen_in_tier1_country) OVER (user_window) AS seen_in_tier1_country,
  LOGICAL_OR(registered) OVER (user_window) AS registered,
FROM
  `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_daily_v2`
WHERE
  submission_date = @submission_date
QUALIFY
  ROW_NUMBER() OVER (user_window) = 1
WINDOW
  user_window AS (
    PARTITION BY
      user_id
    ORDER BY
      user_service_first_daily_flow_info.timestamp ASC
  )
