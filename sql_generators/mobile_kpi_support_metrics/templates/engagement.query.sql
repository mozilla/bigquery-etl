{{ header }}
SELECT
  submission_date,
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  is_mobile,
  adadjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  {% for field in product_specific_attribution_fields %}
    {{ field.name }},
  {% endfor %}
  COUNTIF(is_dau) AS dau,
  COUNTIF(is_wau) AS wau,
  COUNTIF(is_mau) AS mau
FROM
  `{{ project_id }}.{{ dataset }}_derived.engagement_clients_{{ version }}`
WHERE
  submission_date = @submission_date
GROUP BY
  submission_date,
  first_seen_date,
  normalized_channel,
  app_name,
  app_version,
  country,
  locale,
  is_mobile,
  adjust_ad_group,
  adjust_campaign,
  adjust_creative,
  adjust_network,
  {% for field in product_specific_attribution_fields %}
    {{ field.name }}
    {% if not loop.last %},
    {% endif %}
  {% endfor %}
