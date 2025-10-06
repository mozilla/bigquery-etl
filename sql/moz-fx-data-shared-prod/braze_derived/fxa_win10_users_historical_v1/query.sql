WITH current_list AS (
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
  `moz-fx-data-shared-prod.braze_derived.fxa_win10_users_daily_v1` AS daily
LEFT JOIN
  current_list AS historical
  ON historical.fxa_id_sha256 = daily.user_id_sha256
WHERE
  submission_date = @submission_date
  AND daily.email IS NOT NULL
  -- FILTER OUT USERS ALREADY IN THE LIST
  AND historical.fxa_id_sha256 IS NULL
  -- ONLY FOR EMAIL WARMING IN en-US LOCALE
  -- REMOVE THIS FILTER BEFORE 10/14/2025 WHEN FULL CAMPAIGN STARTS
  AND daily.locale = 'en-US'
