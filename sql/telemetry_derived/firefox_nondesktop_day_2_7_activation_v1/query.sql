WITH base AS (
  SELECT
    submission_date,
    CASE
      app_name
    WHEN
      'Fennec'
    THEN
      CONCAT(app_name, ' ', os)
    WHEN
      'Focus'
    THEN
      CONCAT(app_name, ' ', os)
    WHEN
      'Lockbox'
    THEN
      CONCAT('Lockwise ', os)
    WHEN
      'Zerda'
    THEN
      'Firefox Lite'
    ELSE
      app_name
    END
    AS product,
    app_name,
    SPLIT(app_version, '.')[offset(0)] AS app_version,
    os,
    normalized_channel,
    country,
    COUNTIF(udf.pos_of_trailing_set_bit(days_created_profile_bits) = 6) AS new_profiles,
    COUNTIF(
      udf.pos_of_trailing_set_bit(days_created_profile_bits) = 6
      AND BIT_COUNT(days_seen_bits << 1 & udf.bitmask_lowest_7()) > 0
    ) AS day_2_7_activated,
  FROM
    `moz-fx-data-shared-prod.telemetry.nondesktop_clients_last_seen_v1`
  GROUP BY
    product,
    app_name,
    app_version,
    submission_date,
    os,
    normalized_channel,
    country
)
SELECT
  *
FROM
  base
WHERE
  submission_date = @submission_date
  AND product IN (
    -- Fenix and Firefox Preview were previously excluded pending:
    -- https://jira.mozilla.com/browse/DS-696
    'Fenix',
    'Firefox Preview',
    'Fennec Android',
    'Focus Android',
    'Fennec iOS',
    'Focus iOS',
    'Firefox Lite',
    'FirefoxConnect',
    'Lockwise Android'
  )
