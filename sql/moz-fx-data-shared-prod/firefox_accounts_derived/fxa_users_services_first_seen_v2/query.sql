WITH fxa_users_services_daily_new_entries AS (
  SELECT
    user_id,
    service,
    app_version,
    os_name,
    os_version,
    country,
    `language`,
    ua_version,
    ua_browser,
    user_service_first_daily_flow_info,
    user_service_utm_info,
    registered,
  FROM
    firefox_accounts_derived.fxa_users_services_daily_v2
  WHERE
    submission_date = @submission_date
),
existing_entries AS (
  SELECT
    user_id,
    service,
  FROM
    firefox_accounts_derived.fxa_users_services_first_seen_v2
  WHERE
    DATE(submission_date) < @submission_date
)
SELECT
  DATE(@submission_date) AS submission_date,
  new_entries.user_id,
  new_entries.service,
  new_entries.registered AS did_register,
  new_entries.os_name AS first_service_os_name,
  new_entries.os_version AS first_service_os_version,
  new_entries.app_version AS first_service_app_version,
  new_entries.country AS first_service_country,
  new_entries.`language` AS first_service_language,
  new_entries.ua_version AS first_service_ua_version,
  new_entries.ua_browser AS first_service_ua_browser,
  new_entries.user_service_first_daily_flow_info.flow_id AS first_service_flow,
  new_entries.user_service_first_daily_flow_info.`timestamp` AS first_service_flow_timestamp,
  new_entries.user_service_first_daily_flow_info.entrypoint AS first_service_flow_entrypoint,
  new_entries.user_service_utm_info.utm_term AS first_service_flow_utm_term,
  new_entries.user_service_utm_info.utm_medium AS first_service_flow_utm_medium,
  new_entries.user_service_utm_info.utm_source AS first_service_flow_utm_source,
  new_entries.user_service_utm_info.utm_campaign AS first_service_flow_utm_campaign,
  new_entries.user_service_utm_info.utm_content AS first_service_flow_utm_content,
FROM
  fxa_users_services_daily_new_entries AS new_entries
FULL OUTER JOIN
  existing_entries
USING
  (user_id, service)
WHERE
  existing_entries.user_id IS NULL
  AND existing_entries.service IS NULL
