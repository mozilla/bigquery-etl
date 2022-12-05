WITH _current AS (
  SELECT
    `timestamp` AS submission_date,
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
    -- In this raw table, we capture the history of activity over the past
    -- 28 days for each usage criterion as a single 64-bit integer. The
    -- rightmost bit represents whether the user was active in the current day.
    CAST(TRUE AS INT64) AS days_seen_bits,
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_devices_daily_v1`
  WHERE
    DATE(`timestamp`) = @submission_date
    -- Making sure we only use login or registration complete events
    -- just in case any other events got through into
    -- fxa_users_services_devices_daily_v1
    AND ((event_type IN ('fxa_login - complete', 'fxa_reg - complete') AND service IS NOT NULL))
),
  --
_previous AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.firefox_accounts_derived.fxa_users_services_devices_last_seen_v1`
  WHERE
    DATE(submission_date) = DATE_SUB(@submission_date, INTERVAL 1 DAY)
    -- Filter out rows from yesterday that have now fallen outside the 28-day window.
    AND udf.shift_28_bits_one_day(days_seen_bits) > 0
),
combined AS (
  SELECT
    IF(_current.user_id IS NOT NULL, _current, _previous).* REPLACE (
      udf.combine_adjacent_days_28_bits(
        _previous.days_seen_bits,
        _current.days_seen_bits
      ) AS days_seen_bits
    ),
  FROM
    _current
  FULL JOIN
    _previous
  USING
    (user_id, service, device_id)
)
SELECT
  submission_date,
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
  days_seen_bits,
FROM
  combined
WHERE
  user_id IS NOT NULL
  AND service IS NOT NULL
  AND device_id IS NOT NULL
QUALIFY
  ROW_NUMBER() OVER (PARTITION BY user_id, service, device_id ORDER BY `submission_date` DESC) = 1
