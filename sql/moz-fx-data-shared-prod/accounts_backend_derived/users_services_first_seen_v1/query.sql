WITH users_services_daily_new_entries AS (
  SELECT
    user_id_sha256,
    service,
    country,
    registered,
  FROM
    `moz-fx-data-shared-prod.accounts_backend.users_services_daily`
  WHERE
  {% if is_init() %}
    DATE(submission_date) < CURRENT_DATE
  {% else %}
    DATE(submission_date) = @submission_date
  {% endif %}
),
existing_entries AS (
  SELECT
    user_id_sha256,
    service,
  FROM
    `accounts_backend_derived.users_services_first_seen_v1`
  WHERE
  {% if is_init() %}
    FALSE
  {% else %}
    DATE(submission_date) < @submission_date
  {% endif %}
)
SELECT
  @submission_date AS submission_date,
  new_entries.user_id_sha256,
  new_entries.service,
  new_entries.registered,
  new_entries.country AS first_service_country
FROM
  users_services_daily_new_entries AS new_entries
FULL OUTER JOIN
  existing_entries
  USING (user_id_sha256, service)
WHERE
  existing_entries.user_id_sha256 IS NULL
  AND existing_entries.service IS NULL
{% if is_init() %}
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY user_id_sha256, service ORDER BY submission_date ASC) = 1
{% endif %}
