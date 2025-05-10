{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
SELECT
  client_id,
  active_users.submission_date AS first_seen_date,
  channel,
  app_name,
  app_display_version AS app_version,
  country,
  city,
  geo_subdivision,
  locale,
  isp,
  normalized_os AS os,
  normalized_os_version AS os_version,
  device_model,
  device_manufacturer,
  is_mobile,
  {% for attribution_field in product_attribution_fields %}
  attribution.{{ attribution_field }},
  {% endfor %}
  attribution.paid_vs_organic,
  device_type,
FROM
  `{{ project_id }}.{{ dataset }}.active_users` AS active_users
LEFT JOIN
  `{{ project_id }}.{{ dataset }}.attribution_clients` AS attribution
  USING(client_id, channel)
WHERE
  active_users.submission_date < CURRENT_DATE
  AND is_new_profile
  AND is_daily_user
  AND active_users.submission_date = active_users.first_seen_date
