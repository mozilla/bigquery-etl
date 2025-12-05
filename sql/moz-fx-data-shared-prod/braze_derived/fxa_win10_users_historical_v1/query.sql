WITH win10_users AS (
  SELECT DISTINCT
    TO_HEX(SHA256(metrics.string.client_association_uid)) AS fxa_id_sha256,
    FIRST_VALUE(DATE(submission_timestamp)) OVER (
      PARTITION BY
        TO_HEX(SHA256(metrics.string.client_association_uid))
      ORDER BY
        submission_timestamp DESC
    ) AS submission_date,
    FIRST_VALUE(client_info.os) OVER (
      PARTITION BY
        TO_HEX(SHA256(metrics.string.client_association_uid))
      ORDER BY
        submission_timestamp DESC
    ) AS os,
    FIRST_VALUE(client_info.os_version) OVER (
      PARTITION BY
        TO_HEX(SHA256(metrics.string.client_association_uid))
      ORDER BY
        submission_timestamp DESC
    ) AS os_version,
    FIRST_VALUE(client_info.windows_build_number) OVER (
      PARTITION BY
        TO_HEX(SHA256(metrics.string.client_association_uid))
      ORDER BY
        submission_timestamp DESC
    ) AS windows_build_number,
    FIRST_VALUE(client_info.locale) OVER (
      PARTITION BY
        TO_HEX(SHA256(metrics.string.client_association_uid))
      ORDER BY
        submission_timestamp DESC
    ) AS locale
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.fx_accounts`
  WHERE
    DATE(submission_timestamp) = DATE_SUB(@submission_date, INTERVAL 14 day)
    AND `mozfun.norm.windows_version_info`(
      client_info.os,
      client_info.os_version,
      client_info.windows_build_number
    ) = 'Windows 10'
),
seen_within_14_days AS (
  SELECT DISTINCT
    user_id_sha256
  FROM
    `moz-fx-data-shared-prod.accounts_backend_derived.users_services_last_seen_v1`
  WHERE
    submission_date = @submission_date
    AND mozfun.bits28.active_in_range(days_seen_bits, -13, 14)
),
last_seen_14_days AS (
  SELECT DISTINCT
    last_seen.user_id_sha256,
    last_seen.submission_date
  FROM
    `moz-fx-data-shared-prod.accounts_backend_derived.users_services_last_seen_v1` AS last_seen
  LEFT JOIN
    seen_within_14_days AS seen_recently
    ON last_seen.user_id_sha256 = seen_recently.user_id_sha256
  WHERE
    last_seen.submission_date = @submission_date
    -- seen somewhere in days 14-21
    AND mozfun.bits28.active_in_range(last_seen.days_seen_bits, -21, 8)
    -- not seen in last 14 days
    AND NOT mozfun.bits28.active_in_range(last_seen.days_seen_bits, -13, 14)
    -- remove users who have been seen in last 14 days
    AND seen_recently.user_id_sha256 IS NULL
),
inactive_win10_users AS (
  SELECT
    last_seen.submission_date,
    last_seen.user_id_sha256,
    win10.locale
  FROM
    last_seen_14_days AS last_seen
  LEFT JOIN
    win10_users AS win10
    ON last_seen.user_id_sha256 = win10.fxa_id_sha256
  -- filter out users that don't have an FX account
  WHERE
    win10.fxa_id_sha256 IS NOT NULL
),
current_day_users_to_add AS (
  SELECT
    inactive.submission_date,
    braze_users.external_id AS external_id,
  -- if user is in our braze users table use their email, otherwise use the email associated with their fxa_id
    IFNULL(braze_users.email, fxa_emails.normalizedEmail) AS email,
    inactive.user_id_sha256,
    inactive.locale
  FROM
    inactive_win10_users AS inactive
  LEFT JOIN
    `moz-fx-data-shared-prod.braze_derived.users_v1` AS braze_users
    ON inactive.user_id_sha256 = braze_users.fxa_id_sha256
  LEFT JOIN
    `moz-fx-data-shared-prod.accounts_backend_external.emails_v1` AS fxa_emails
    ON inactive.user_id_sha256 = TO_HEX(SHA256(fxa_emails.uid))
  -- some users have multiple email addresses in this table, only use primary
    AND fxa_emails.isPrimary = TRUE
),
current_list AS (
  SELECT
    fxa_id_sha256
  FROM
    `moz-fx-data-shared-prod.braze_derived.fxa_win10_users_historical_v1`
)
SELECT
  daily.submission_date,
  IFNULL(daily.external_id, TO_HEX(SHA256(GENERATE_UUID()))) AS external_id,
  daily.email,
  daily.locale,
  daily.user_id_sha256 AS fxa_id_sha256
FROM
  current_day_users_to_add AS daily
LEFT JOIN
  current_list AS historical
  ON historical.fxa_id_sha256 = daily.user_id_sha256
WHERE
  submission_date = @submission_date
  AND daily.email IS NOT NULL
  -- FILTER OUT USERS ALREADY IN THE LIST
  AND historical.fxa_id_sha256 IS NULL
  AND daily.locale IN ('de', 'en-US', 'en-GB', 'es-ES', 'fr', 'it', 'pl', 'pt-BR')
