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
  FROM
    `{{ project_id }}.{{ dataset }}.active_users`
)
{% if product_attribution_fields %}
  , attribution AS (
    SELECT
      client_id,
      sample_id,
      channel AS normalized_channel,
      {% for attribution_field in product_attribution_fields.values() if not attribution_field.name.endswith("_timestamp") %}
        {% if app_name == "fenix" and attribution_field.name == "adjust_campaign" %}
          CASE
            WHEN adjust_network IN ('Google Organic Search', 'Organic')
              THEN 'Organic'
            ELSE NULLIF(adjust_campaign, "")
          END AS adjust_campaign,
        {% elif attribution_field.type == "STRING" %}
          NULLIF({{ attribution_field.name }}, "") AS {{ attribution_field.name }},
        {% else %}
          {{ attribution_field.name }},
        {% endif %}
      {% endfor %}
    FROM
      {% if app_name == "fenix" %}
        `{{ project_id }}.{{ dataset }}_derived.firefox_android_clients_v1`
      {% elif app_name == "firefox_ios" %}
        `{{ project_id }}.{{ dataset }}_derived.firefox_ios_clients_v1`
      {% endif %}
  )
{% endif %}
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
  {% for attribution_field in product_attribution_fields.values() if not attribution_field.name.endswith("_timestamp") %}
  attribution.{{ attribution_field.name }},
  {% endfor %}
  {% if 'adjust_network' in product_attribution_fields %}
    `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
  {% else %}
    "Organic" AS paid_vs_organic,
  {% endif %}
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
FROM
  `{{ project_id }}.{{ dataset }}.baseline_clients_daily` AS clients_daily
INNER JOIN
  active_users
  ON clients_daily.submission_date = active_users.retention_seen.day_27.metric_date
  AND clients_daily.client_id = active_users.client_id
  AND clients_daily.normalized_channel = active_users.normalized_channel
{% if product_attribution_fields %}
  LEFT JOIN
    attribution
    ON clients_daily.client_id = attribution.client_id
      AND clients_daily.normalized_channel = attribution.normalized_channel
{% endif %}
WHERE
  active_users.retention_seen.day_27.active_on_metric_date
