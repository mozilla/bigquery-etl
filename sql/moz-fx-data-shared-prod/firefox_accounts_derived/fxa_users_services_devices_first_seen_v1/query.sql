WITH new_device_entries AS (
  SELECT
    `timestamp`,
    user_id,
    service,
    device_id,
    os_name,
    flow_id,
    event_type,
    country,
    `language`,
    entrypoint,
    utm_term,
    utm_medium,
    utm_source,
    utm_campaign,
    utm_content,
    ua_version,
    ua_browser,
  FROM
    firefox_accounts_derived.fxa_users_services_devices_daily_v1
  WHERE
    DATE(`timestamp`) = @submission_date
),
existing_devices AS (
  SELECT DISTINCT
    CONCAT(user_id, service, device_id) AS existing_entry
  FROM
    firefox_accounts_derived.fxa_users_services_devices_first_seen_v1
  -- in case we backfill we want to exclude entries newer than submission_date
  -- so that we can recalculate those partitions
  WHERE
    DATE(first_seen_date) < @submission_date
)
SELECT
  `timestamp` AS first_seen_date,
  user_id,
  service,
  device_id,
  os_name,
  flow_id,
  event_type,
  country,
  `language`,
  entrypoint,
  utm_term,
  utm_medium,
  utm_source,
  utm_campaign,
  utm_content,
  ua_version,
  ua_browser,
FROM
  new_device_entries
WHERE
  CONCAT(user_id, service, device_id) NOT IN (SELECT existing_entry FROM existing_devices)
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY user_id, service, device_id ORDER BY `timestamp` ASC) = 1
