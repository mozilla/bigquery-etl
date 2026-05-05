SELECT
  *
FROM
  `moz-fx-data-shared-prod.subscription_platform_derived.monthly_active_logical_subscriptions_v1_live`
WHERE
  {% if is_init() %}
    month_start_date <= CURRENT_DATE() - 8
  {% else %}
    month_start_date = DATE_TRUNC(@date, MONTH)
  {% endif %}
