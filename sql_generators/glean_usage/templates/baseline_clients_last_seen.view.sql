{{ header }}
{% set all_kpi_products = [
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
  {% if app_name in all_kpi_products %}
  -- Metrics based on activity
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
  mozfun.bits28.days_since_seen(days_active_bits) = 0 AS is_dau,
  mozfun.bits28.days_since_seen(days_active_bits) < 7 AS is_wau,
  mozfun.bits28.days_since_seen(days_active_bits) < 28 AS is_mau,
  -- Metrics based on pings sent
  mozfun.bits28.days_since_seen(days_seen_bits) = 0 AS is_daily_user,
  mozfun.bits28.days_since_seen(days_seen_bits) < 7 AS is_weekly_user,
  mozfun.bits28.days_since_seen(days_seen_bits) < 28 AS is_monthly_user,
  CASE
      WHEN LOWER(isp) = 'browserstack'
        THEN CONCAT("{{ app_name }}", ' ', isp)
      {% if app_name in ["fenix"] %}
      WHEN LOWER(distribution_id) = 'mozillaonline'
        THEN CONCAT("{{ app_name }}", ' ', distribution_id)
      {% endif %}
      ELSE "{{ app_name }}"
  END AS app_name,
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
