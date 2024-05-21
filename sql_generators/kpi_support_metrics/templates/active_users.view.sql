{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
SELECT
  *,
  CASE
    WHEN LOWER(isp) = 'browserstack'
      THEN CONCAT("{{ friendly_name }}", ' ', isp)
    ELSE "{{ friendly_name }}"
  END AS app_name,
  -- Activity fields to support metrics built on top of activity
  CASE
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 1
      AND 6
      THEN 'infrequent_user'
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 7
      AND 13
      THEN 'casual_user'
    WHEN BIT_COUNT(days_active_bits)
      BETWEEN 14
      AND 20
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
  {% if is_mobile_kpi %}
    (
      LOWER(IFNULL(isp, "")) <> "browserstack"
      {% if has_mozilla_online %}
        AND LOWER(IFNULL(distribution_id, "")) <> "mozillaonline"
      {% endif %}
    )
  {% else %}
    FALSE
  {% endif %} AS is_mobile,
  {% if is_desktop_kpi %}
    TRUE
  {% else %}
    FALSE
  {% endif %} AS is_desktop,
FROM
  `{{ project_id }}.{{ dataset }}.baseline_clients_last_seen`
