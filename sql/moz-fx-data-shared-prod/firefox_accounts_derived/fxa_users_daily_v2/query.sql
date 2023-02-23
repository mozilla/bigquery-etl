SELECT
  submission_date,
  user_id,
  country,
  `language`,
  app_version,
  os_name,
  os_version,
  seen_in_tier1_country,
  registered,
FROM
  `firefox_accounts_derived.fxa_users_services_daily_v2`
WHERE
  submission_date = @submission_date
QUALIFY
  ROW_NUMBER() OVER (
    PARTITION BY
      user_id
    ORDER BY
      user_service_first_daily_flow_info.`timestamp` ASC
  ) = 1
