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
    isp,
    app_display_version,
    is_dau,
    is_wau,
    is_mau,
    is_mobile,
  FROM
    `{{ project_id }}.{{ dataset }}.active_users`
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
  isp,
  is_dau,
  is_wau,
  is_mau,
  is_mobile,
  {% for attribution_field in product_attribution_fields.values() %}
    {% if app_name == "fenix" and attribution_field.name == "adjust_campaign" %}
      CASE
        WHEN attribution.adjust_network IN ('Google Organic Search', 'Organic')
          THEN 'Organic'
        ELSE NULLIF(attribution.adjust_campaign, "")
      END AS adjust_campaign,
    {% elif attribution_field.type == "STRING" %}
      NULLIF(attribution.{{ attribution_field.name }}, "") AS {{ attribution_field.name }},
    {% else %}
      attribution.{{ attribution_field.name }},
    {% endif %}
  {% endfor %}
  {% if 'adjust_network' in product_attribution_fields %}
    `moz-fx-data-shared-prod.udf.organic_vs_paid_mobile`(attribution.adjust_network) AS paid_vs_organic,
  {% else %}
    "Organic" AS paid_vs_organic,
  {% endif %}
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
FROM
  active_users
  {% if product_attribution_fields %}
    LEFT JOIN
      `{{ project_id }}.{{ dataset }}.attribution_clients` AS attribution
      USING(client_id)
  {% endif %}
