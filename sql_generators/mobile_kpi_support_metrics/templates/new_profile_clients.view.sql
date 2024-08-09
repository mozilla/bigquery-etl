{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
SELECT
  client_id,
  active_users.submission_date AS first_seen_date,
  normalized_channel,
  app_name,
  app_display_version AS app_version,
  country,
  locale,
  isp,
  normalized_os AS os,
  normalized_os_version AS os_version,
  device_model,
  device_manufacturer,
  is_mobile,
  {% for attribution_field in product_attribution_fields.values() if not attribution_field.name.endswith("_timestamp") %}
  attribution.{{ attribution_field.name }},
  {% endfor %}
  {% if 'adjust_network' in product_attribution_fields %}
    `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
  {% else %}
    "Organic" AS paid_vs_organic,
  {% endif %}
FROM
  `{{ project_id }}.{{ dataset }}.active_users` AS active_users
LEFT JOIN
  `{{ project_id }}.{{ dataset }}.attribution_clients` AS attribution
  USING(client_id)
WHERE
  active_users.submission_date < CURRENT_DATE
  AND is_new_profile
  AND is_daily_user
