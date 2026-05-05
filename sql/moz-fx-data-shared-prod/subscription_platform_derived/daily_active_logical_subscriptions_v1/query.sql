SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.daily_active_logical_subscriptions_v1_live`
WHERE
  {% if is_init() %}
    `date` <= CURRENT_DATE() - 8
  {% else %}
    `date` = @date
  {% endif %}
