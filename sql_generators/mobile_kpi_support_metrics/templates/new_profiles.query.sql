{{ header }}
SELECT
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  is_mobile,
  {% for field in product_attribution_fields.values() %}
    {{ field.name }},
  {% endfor %}
  COUNT(*) AS new_profiles,
FROM
  `{{ project_id }}.{{ dataset }}.new_profile_clients`
WHERE
  {% raw %}
  {% if is_init() %}
    first_seen_date < CURRENT_DATE
  {% else %}
    first_seen_date = @submission_date
  {% endif %}
  {% endraw %}
GROUP BY
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  is_mobile
  {% for field in product_attribution_fields.values() %}
    {% if loop.first %},
    {% endif %}
    {{ field.name }}
    {% if not loop.last %},
    {% endif %}
  {% endfor %}
