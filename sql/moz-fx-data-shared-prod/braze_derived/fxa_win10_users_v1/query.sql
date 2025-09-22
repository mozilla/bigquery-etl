WITH win10_users AS (
  SELECT
    TO_HEX(SHA256(metrics.string.client_association_uid)) AS fxa_id_sha256,
    client_info.os AS os,
    client_info.os_version AS os_version
  FROM
    `moz-fx-data-shared-prod.firefox_desktop.fx_accounts`
  WHERE
    DATE(submission_timestamp) = @submission_date
    AND client_info.os = 'Windows'
    AND client_info.os_version = '10.0'
  GROUP BY
    1,
    2,
    3
),
last_seen_14_days AS (
  SELECT
    *
  FROM
    `moz-fx-data-shared-prod.accounts_backend_derived.users_services_last_seen_v1`
  WHERE
    submission_date = @submission_date
    -- bit pattern 10000000000000, last seen 14 days from submission date
    AND days_seen_bits = 8192
),
inactive_win10_users AS (
  SELECT
    last_seen.user_id_sha256,
    win10.os,
    win10.os_version,
    last_seen.days_seen_bits
  FROM
    last_seen_14_days AS last_seen
  LEFT JOIN
    win10_users AS win10
    ON last_seen.user_id_sha256 = win10.fxa_id_sha256
  -- filter out users that don't have an FX account
  WHERE
    win10.fxa_id_sha256 IS NOT NULL
)
SELECT
  inactive.submission_date,
  -- if user is in our braze users table use their external_id, otherwise generate a braze external_id
  IFNULL(braze_users.external_id, GENERATE_UUID()) AS external_id,
  -- if user is in our braze users table use their email, otherwise use the email associated with their fxa_id
  IFNULL(braze_users.email, fxa_emails.normalizedEmail) AS email,
  inactive.fxa_id_sha256
FROM
  inactive_win10_users AS inactive
LEFT JOIN
  `moz-fx-data-shared-prod.braze_derived.users_v1` AS braze_users
  ON inactive.user_id_sha256 = braze_users.fxa_id_sha256
LEFT JOIN
  `moz-fx-data-shared-prod.accounts_db_external.fxa_emails_v1` AS fxa_emails
  ON inactive.fxa_id_sha256 = fxa_emails.uid
