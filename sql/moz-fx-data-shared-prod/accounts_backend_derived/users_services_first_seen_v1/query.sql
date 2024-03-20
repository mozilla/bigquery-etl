WITH users_services_daily_new_entries AS (
  SELECT
    user_id_sha256,
    service,
    country,
    registered
  FROM
    `moz-fx-data-shared-prod.accounts_backend_derived.users_services_daily_v1`
  WHERE
    submission_date = @submission_date
),
existing_entries AS (
  SELECT
    user_id_sha256,
    service,
  FROM
    `accounts_backend_derived.users_services_first_seen_v1`
  WHERE
    DATE(submission_date) < @submission_date
)
SELECT
  DATE(@submission_date) AS submission_date,
  new_entries.user_id_sha256,
  new_entries.service,
  new_entries.registered AS did_register,
  new_entries.country AS first_service_country
FROM
  users_services_daily_new_entries AS new_entries
FULL OUTER JOIN
  existing_entries
  USING (user_id_sha256, service)
WHERE
  existing_entries.user_id_sha256 IS NULL
  AND existing_entries.service IS NULL