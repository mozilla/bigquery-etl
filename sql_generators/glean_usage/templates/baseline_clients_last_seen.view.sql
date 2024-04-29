{{ header }}
{% set products_to_include_extra_activity_fields = [
  "firefox_ios",
  "focus_ios",
  "klar_ios",
  "fenix",
  "focus_android",
  "klar_android",
  "firefox_desktop"
] %}
{% set mobile_kpi_products = [
  "firefox_ios",
  "focus_ios",
  "fenix",
  "focus_android",
] %}

CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ last_seen_view }}`
AS
SELECT
  {% for ut in usage_types %}
    `moz-fx-data-shared-prod`.udf.pos_of_trailing_set_bit(days_{{ ut }}_bits) AS days_since_{{ ut }},
  {% endfor %}
  last_seen.*,
  {% if app_name in products_to_include_extra_activity_fields %}
  CASE
      WHEN LOWER(isp) = 'browserstack'
        THEN CONCAT("{{ app_name }}", ' ', isp)
      {% if app_name in ["fenix"] %}
      WHEN LOWER(distribution_id) = 'mozillaonline'
        THEN CONCAT("{{ app_name }}", ' ', distribution_id)
      {% endif %}
      ELSE "{{ app_name }}"
  END AS app_name,
  -- Activity fields to support metrics built on top of activity
  CASE
    WHEN BIT_COUNT(days_active_bits)
        BETWEEN 1 AND 6
            THEN 'infrequent_user'
    WHEN BIT_COUNT(days_active_bits)
        BETWEEN 7 AND 13
            THEN 'casual_user'
    WHEN BIT_COUNT(days_active_bits)
        BETWEEN 14 AND 20
            THEN 'regular_user'
    WHEN BIT_COUNT(days_active_bits) >= 21
        THEN 'core_user'
    ELSE 'other'
  END AS activity_segment,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) = 0, FALSE) AS is_dau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 7, FALSE) AS is_wau,
  IFNULL(mozfun.bits28.days_since_seen(days_active_bits) < 28, FALSE) AS is_mau,
  -- Metrics based on pings sent
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) = 0, FALSE) AS is_daily_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 7, FALSE) AS is_weekly_user,
  IFNULL(mozfun.bits28.days_since_seen(days_seen_bits) < 28, FALSE) AS is_monthly_user,
  {% if app_name in mobile_kpi_products %}
  (
    LOWER(IFNULL(isp, "")) <> "browserstack"
    {% if app_name in ["fenix"] %}
    AND LOWER(IFNULL(distribution_id, "")) <> "mozillaonline"
    {% endif %}
  )
  {% else %}
  FALSE
  {% endif %} AS is_mobile,  -- Indicates which records should be used for mobile KPI metric calculations.
  FALSE AS is_desktop, -- Always FALSE as legacy telemetry is used for desktop KPI calculations for now.
{% endif %}
FROM
  `{{ project_id }}.{{ last_seen_table }}` AS last_seen
{% if app_name == "fenix" %}
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.firefox_android_clients` USING(client_id)
{% endif -%}
