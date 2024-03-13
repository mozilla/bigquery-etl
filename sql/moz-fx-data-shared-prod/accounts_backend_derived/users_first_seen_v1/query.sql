WITH users_services_daily_new_entries AS (
  SELECT DISTINCT
    user_id_sha256,
    country,
    seen_in_tier1_country,
    registered
  FROM
    `accounts_backend_derived.users_services_daily_v1`
  WHERE
    submission_date = @submission_date
),
existing_entries AS (
  SELECT
    user_id_sha256
  FROM
    `accounts_backend_derived.users_first_seen_v1`
  WHERE
    DATE(submission_date) < @submission_date
)
SELECT
  new_entries.*,
  DATE(@submission_date) AS first_seen_date,
FROM
  users_services_daily_new_entries AS new_entries
FULL OUTER JOIN
  existing_entries
  USING (user_id_sha256)
WHERE
  existing_entries.user_id_sha256 IS NULL
