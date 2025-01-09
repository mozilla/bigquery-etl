{{ header }}
WITH device_manufacturer_counts AS (
  SELECT
    submission_date,
    device_manufacturer,
    COUNT(*) AS device_manufacturer_count,
  FROM
  `{{ project_id }}.{{ dataset }}.engagement_clients`
  WHERE
    {% raw %}
    {% if is_init() %}
      submission_date < CURRENT_DATE
    {% else %}
      submission_date = @submission_date
    {% endif %}
    {% endraw %}
  GROUP BY
    submission_date,
    device_manufacturer
)

SELECT
  submission_date,
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  is_mobile,
  {% for field in product_attribution_fields.values() if not field.client_only %}
  {{ field.name }},
  {% endfor %}
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau,
  device_type,
  -- Bucket device manufacturers with low count prior to aggregation
  IF(device_manufacturer_count <= 2000, "other", device_manufacturer) AS device_manufacturer,
FROM
  `{{ project_id }}.{{ dataset }}.engagement_clients`
LEFT JOIN
  device_manufacturer_counts
  USING(submission_date, device_manufacturer)
WHERE
  {% raw %}
  {% if is_init() %}
    submission_date < CURRENT_DATE
  {% else %}
    submission_date = @submission_date
  {% endif %}
  {% endraw %}
GROUP BY
  submission_date,
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  device_type,
  device_manufacturer,
  is_mobile
  {% for field in product_attribution_fields.values() if not field.client_only %}
    {% if loop.first %},{% endif %}
    {{ field.name }}
    {% if not loop.last %},{% endif %}
  {% endfor %}
