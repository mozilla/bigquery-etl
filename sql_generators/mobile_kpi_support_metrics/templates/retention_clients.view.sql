{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
WITH active_users AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    app_name,
    normalized_channel,
    mozfun.bits28.retention(days_seen_bits, submission_date) AS retention_seen,
    mozfun.bits28.retention(days_active_bits & days_seen_bits, submission_date) AS retention_active,
    days_seen_bits,
    days_active_bits,
    is_mobile,
    device_type,
    device_manufacturer,
  FROM
    `{{ project_id }}.{{ dataset }}.active_users`
),
attribution AS (
  SELECT
    client_id,
    sample_id,
    {% for attribution_field in product_attribution_fields %}
    {{ attribution_field }},
    {% endfor %}
    paid_vs_organic,
  FROM
    `{{ project_id }}.{{ dataset }}.attribution_clients`
)
SELECT
  active_users.submission_date AS submission_date,
  clients_daily.submission_date AS metric_date,
  clients_daily.first_seen_date,
  clients_daily.client_id,
  clients_daily.sample_id,
  active_users.app_name,
  clients_daily.normalized_channel,
  clients_daily.country,
  clients_daily.app_display_version AS app_version,
  clients_daily.locale,
  clients_daily.isp,
  active_users.is_mobile,
  {% for attribution_field in product_attribution_fields %}
  attribution.{{ attribution_field }},
  {% endfor %}
  attribution.paid_vs_organic,
  -- ping sent retention
  active_users.retention_seen.day_27.active_on_metric_date AS ping_sent_metric_date,
  (
    active_users.retention_seen.day_27.active_on_metric_date
    AND active_users.retention_seen.day_27.active_in_week_3
  ) AS ping_sent_week_4,
  -- activity retention
  active_users.retention_active.day_27.active_on_metric_date AS active_metric_date,
  (
    active_users.retention_active.day_27.active_on_metric_date
    AND active_users.retention_active.day_27.active_in_week_3
  ) AS retained_week_4,
  -- new profile retention
  clients_daily.is_new_profile AS new_profile_metric_date,
  (
    clients_daily.is_new_profile
    AND active_users.retention_active.day_27.active_in_week_3
  ) AS retained_week_4_new_profile,
  (
    clients_daily.is_new_profile
    -- Looking back at 27 days to support the official definition of repeat_profile (someone active between days 2 and 28):
    AND BIT_COUNT(mozfun.bits28.range(active_users.days_active_bits, -26, 27)) > 0
  ) AS repeat_profile,
  active_users.days_seen_bits,
  active_users.days_active_bits,
  CASE
    WHEN clients_daily.submission_date = first_seen_date
      THEN 'new_profile'
    WHEN DATE_DIFF(clients_daily.submission_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_user'
    WHEN DATE_DIFF(clients_daily.submission_date, first_seen_date, DAY) >= 28
      THEN 'existing_user'
    ELSE 'Unknown'
  END AS lifecycle_stage,
  active_users.device_type,
  active_users.device_manufacturer,
FROM
  `{{ project_id }}.{{ dataset }}.baseline_clients_daily` AS clients_daily
INNER JOIN
  active_users
  ON clients_daily.submission_date = active_users.retention_seen.day_27.metric_date
  AND clients_daily.client_id = active_users.client_id
  AND clients_daily.normalized_channel = active_users.normalized_channel
LEFT JOIN
  attribution
  ON clients_daily.client_id = attribution.client_id
  AND clients_daily.sample_id = attribution.sample_id
WHERE
  active_users.retention_seen.day_27.active_on_metric_date
