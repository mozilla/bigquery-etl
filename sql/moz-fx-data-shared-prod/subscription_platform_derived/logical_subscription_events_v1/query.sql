SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.logical_subscription_events_v1_live`
WHERE
  {% if is_init() %}
    DATE(`timestamp`) <= CURRENT_DATE() - 8
  {% else %}
    DATE(`timestamp`) = @date
  {% endif %}
