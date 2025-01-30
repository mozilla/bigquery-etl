{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
WITH active_users AS (
  SELECT
    submission_date,
    client_id,
    sample_id,
    first_seen_date,
    app_name,
    normalized_channel,
    locale,
    country,
    city,
    geo_subdivision,
    isp,
    app_display_version,
    is_dau,
    is_wau,
    is_mau,
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
  submission_date,
  client_id,
  sample_id,
  first_seen_date,
  app_name,
  normalized_channel,
  app_display_version AS app_version,
  locale,
  country,
  city,
  geo_subdivision,
  isp,
  is_dau,
  is_wau,
  is_mau,
  is_mobile,
  {% for attribution_field in product_attribution_fields %}
  attribution.{{ attribution_field }},
  {% endfor %}
  attribution.paid_vs_organic,
  CASE
    WHEN active_users.submission_date = first_seen_date
      THEN 'new_profile'
    WHEN DATE_DIFF(active_users.submission_date, first_seen_date, DAY)
      BETWEEN 1
      AND 27
      THEN 'repeat_user'
    WHEN DATE_DIFF(active_users.submission_date, first_seen_date, DAY) >= 28
      THEN 'existing_user'
    ELSE 'Unknown'
  END AS lifecycle_stage,
  device_type,
  device_manufacturer,
FROM
  active_users
LEFT JOIN
  attribution
  USING(client_id, sample_id)
