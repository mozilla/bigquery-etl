{{ header }}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ last_seen_view }}`
AS
SELECT
  {% for ut in usage_types %}
    `moz-fx-data-shared-prod`.udf.pos_of_trailing_set_bit(days_{{ ut }}_bits) AS days_since_{{ ut }},
  {% endfor %},
  -- Metrics based on activity
  mozfun.bits28.days_since_seen(days_active_bits) = 0 AS is_dau,
  mozfun.bits28.days_since_seen(days_active_bits) < 7 AS is_wau,
  mozfun.bits28.days_since_seen(days_active_bits) < 28 AS is_mau,
  -- Metrics based on pings sent
  mozfun.bits28.days_since_seen(days_seen_bits) = 0 AS is_daily_user,
  mozfun.bits28.days_since_seen(days_seen_bits) < 7 AS is_weekly_user,
  mozfun.bits28.days_since_seen(days_seen_bits) < 28 AS is_monthly_user,
  CASE
    WHEN LOWER(isp) = 'browserstack'
      THEN CONCAT(app_name, ' ', isp_name)
    {% if app_name = "fenix" %}
    WHEN LOWER(distribution_id) = 'mozillaonline'
      THEN CONCAT(app_name, ' ', distribution_id)
    {% endif %}
    ELSE app_name
  END AS analysis_app_name,
  NOT (
    LOWER(isp) = "browserstack"
    {% if app_name = "fenix" %}
    AND LOWER(distribution_id) = "mozillaonline"
    {% endif %}
  ) AS is_mobile, -- TODO: add logic to check the app is a mobile app
  FALSE AS is_desktop, -- TODO: add logic to check the app is desktop
  *
FROM
  `{{ project_id }}.{{ last_seen_table }}`
{% if app_name = "fenix" %}
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.firefox_android_clients` USING(client_id)
{% endif %}
