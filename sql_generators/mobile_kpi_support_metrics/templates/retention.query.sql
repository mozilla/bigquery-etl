{{ header }}
SELECT
  metric_date,
  first_seen_date,
  app_name,
  normalized_channel,
  country,
  app_version,
  locale,
  is_mobile,
  {% for field in product_attribution_fields.values() if not field.client_only %}
  {{ field.name }},
  {% endfor %}
  COUNTIF(ping_sent_metric_date) AS ping_sent_metric_date,
  COUNTIF(ping_sent_week_4) AS ping_sent_week_4,
  COUNTIF(active_metric_date) AS active_metric_date,
  COUNTIF(retained_week_4) AS retained_week_4,
  COUNTIF(retained_week_4_new_profile) AS retained_week_4_new_profiles,
  COUNTIF(new_profile_metric_date) AS new_profiles_metric_date,
  COUNTIF(repeat_profile) AS repeat_profiles,
  device_type,
  device_manufacturer,
FROM
  `{{ project_id }}.{{ dataset }}.retention_clients`
WHERE
  {% raw %}
  {% if is_init() %}
    metric_date < DATE_SUB(CURRENT_DATE, INTERVAL 27 DAY)
    AND submission_date < CURRENT_DATE
  {% else %}
    metric_date = DATE_SUB(@submission_date, INTERVAL 27 DAY)
    AND submission_date = @submission_date
  {% endif %}
  {% endraw %}
GROUP BY
  metric_date,
  first_seen_date,
  app_name,
  normalized_channel,
  country,
  app_version,
  locale,
  device_type,
  device_manufacturer,
  is_mobile
  {% for field in product_attribution_fields.values() if not field.client_only %}
    {% if loop.first %},{% endif %}
    {{ field.name }}
    {% if not loop.last %},{% endif %}
  {% endfor %}
