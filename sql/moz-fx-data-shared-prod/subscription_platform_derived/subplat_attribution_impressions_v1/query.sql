SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.subplat_attribution_impressions_v1_live`
WHERE
  {% if is_init() %}
    DATE(impression_at) <= CURRENT_DATE() - 2
  {% else %}
    DATE(impression_at) = @date
  {% endif %}
