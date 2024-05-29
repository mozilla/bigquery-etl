{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
SELECT
  client_id,
  first_seen_date,
  normalized_channel,
  app_name,
  app_display_version AS app_version,
  country,
  locale,
  isp,
  is_mobile,
  {% for field in product_attribution_fields.values() %}
    attribution.{{ field.name }},
  {% endfor %}
  {% if 'adjust_network' in product_attribution_fields %}
    `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(adjust_network) AS paid_vs_organic,
  {% else %}
    "Organic" AS paid_vs_organic,
  {% endif %}
FROM
  `{{ project_id }}.{{ dataset }}.active_users`
LEFT JOIN
  `{{ project_id }}.{{ dataset }}.attribution_clients` AS attribution
  USING(client_id)
WHERE
  submission_date < CURRENT_DATE
  AND is_new_profile
