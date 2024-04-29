-- Generated via ./bqetl generate glean_usage

CREATE OR REPLACE VIEW
    `{{ project_id }}.{{ target_view }}`
AS
{% for (dataset, channel) in datasets -%}
{% if not loop.first -%}
UNION ALL
{% endif -%}
{% if app_name == "fenix" -%}
SELECT
    "{{ dataset }}" AS normalized_app_id,
    * REPLACE(mozfun.norm.fenix_app_info("{{ dataset }}", app_build).channel AS normalized_channel),
{% else -%}
SELECT
    "{{ dataset }}" AS normalized_app_id,
    * REPLACE("{{ channel }}" AS normalized_channel),
{% endif -%}
{% if table == "baseline_clients_last_seen" %}
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
    {% if app_name in ["klar_ios", "klar_android", "firefox_desktop"] -%}
    FALSE
    {% else -%}
    (
        LOWER(IFNULL(isp, "")) <> "browserstack"
        {% if app_name == "fenix" %}
        AND LOWER(IFNULL(distribution_id, "")) <> "mozillaonline"
        {% endif %}
    ){% endif %} AS is_mobile,  -- determined if should be used towards KPI calculations
FROM `{{ project_id }}.{{ dataset }}.{{ table }}`
{% if app_name == "fenix" %}
LEFT JOIN
  `moz-fx-data-shared-prod.fenix.firefox_android_clients` USING(client_id)
{% endif %}
{% endif -%}
{% endfor %}
