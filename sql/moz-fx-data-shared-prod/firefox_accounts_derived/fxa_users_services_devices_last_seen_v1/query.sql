WITH _current AS (
  SELECT
    `timestamp` AS event_timestamp,
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
    firefox_accounts_derived.fxa_users_services_devices_daily_v1
  WHERE
    DATE(`timestamp`) = @submission_date
    AND event_type IN (
      'fxa_activity - access_token_checked',
      'fxa_activity - access_token_created',
      'fxa_activity - cert_signed'
    )
),
_previous AS (
  SELECT
    * EXCEPT (submission_date)
  FROM
    firefox_accounts_derived.fxa_users_services_devices_last_seen_v1
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
  -- Retaining `timestamp` to represent when the last event took place
  -- and adding submission_date for correct partitioning in BigQuery.
  @submission_date AS submission_date,
  event_timestamp,
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
  ROW_NUMBER() OVER (PARTITION BY user_id, service, device_id ORDER BY `event_timestamp` DESC) = 1
